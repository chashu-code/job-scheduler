require Lager

defmodule Job.Scheduler do
  use GenServer

  alias Job.Util
  alias Job.Dispatcher
  alias Job.ScheduleLimiter

  def start_link(name, info, opts \\ []) do
    state = ensure_schedule_info(name, info)

    GenServer.start_link(__MODULE__, state, opts)
  end

  def init(state) do
    {:ok, state, 0}
  end

  def ensure_schedule_info(name, %{}=info) do
    info = info
            |> Dict.put_new(:name, name)
            |> Dict.put_new(:state, :standby)
            |> Dict.put_new(:interval, {15, :mins})
            |> Dict.put_new(:allow_ats, nil)
            |> Dict.put_new(:schedule_at, nil)
            |> Dict.put_new(:dispatchers, [])
            |> Dict.put_new(:flows, [])


    settings = Application.get_env(:job_scheduler, :settings, [])

    info
    |> Dict.put(:flows_copy, info.flows)
    |> Dict.put(:parallel_max, settings[:parallel_max] || 0)
    |> Dict.put(:notifier, settings[:notifier])
  end


  def handle_info(:timeout, state) do
    # set timeout check interval
    timeout_check_ms = state.interval
                       |> Util.to_ms
                       |> div(10)
                       |> max(1)
                       |> min(30_000)
    # 因为加了schedule 并发限制
    # 所以，最长检测时间变更为 30s

    state = state
            |> Dict.put(:timeout_check_ms, timeout_check_ms)
            |> schedule

    :erlang.send_after timeout_check_ms, self, :timeout_check
    {:noreply, state}
  end



  def handle_info(:timeout_check, state) do
   state = schedule(state)
    :erlang.send_after state.timeout_check_ms, self, :timeout_check
    {:noreply, state}
  end

  def handle_info({:dispatcher_start, _dispatcher}, state), do: {:noreply, state}
  def handle_info({:dispatcher_restart, _dispatcher}, state), do: {:noreply, state}
  def handle_info({:dispatcher_pause, _dispatcher}, state), do: {:noreply, state}
  def handle_info({:dispatcher_stop, _dispatcher}, state), do: {:noreply, state}
  def handle_info({:dispatcher_finish, dispatcher}, state) do
    state_new = case List.delete(state.dispatchers, dispatcher) do
      []  ->
        state
        |> Dict.put(:dispatchers, [])
        |> flows_next
      dispatchers ->
        Dict.put state, :dispatchers, dispatchers
    end

    {:noreply, state_new}
  end

  def start_dispatchers(state, []), do: state
  def start_dispatchers(state, [{mod, args} | flow]) do
    {:ok, dpid} = Dispatcher.start_link
    Dispatcher.start dpid, %{
      job_make: {mod, args},
      evt_receiver: self
    }
    state
    |> Dict.put(:dispatchers, [dpid | state.dispatchers])
    |> start_dispatchers(flow)
  end

  def state_change(state, state_next) do
    Lager.info "~p job scheduler's state change: ~p", [state.name, state_next]
    state = Dict.put state, :state, state_next
    notify(state)
    state
  end

  def terminate(reason, state) do
    notify(state, inspect(reason))
  end

  def notify(state, reason \\ nil) do
    # release hold
    if state.state == :finish || not is_nil(reason) do
      ScheduleLimiter.release
    end

    notifier = state.notifier
    case notifier do
      {m, f} -> # 定义了正确的 notifier
        args = case reason do
                  nil -> # state change
                    [state.name, state.state, "状态变更"]
                  error -> # has error
                    [state.name, :error, error]
                end
        apply(m, f, args)
      _ -> nil
    end
  end

  def schedule(state) do
    if schedule_able?(state.schedule_at, state.interval, state.allow_ats) do
      case state.state do
        flag when flag == :standby or flag == :finish ->
          state
          |> Dict.put(:flows, state.flows_copy)
          |> Dict.put(:schedule_at, Util.ms_now)
          |> state_change(:run)
          |> flows_next
        :run ->
          # timeout
          raise Job.Error, message: "#{state.name} job scheduler timeout."
      end
    else
      state
    end
  end



  def schedule_able?(schedule_at, interval, allow_ats) do
    chk_allow_ats = case allow_ats do
      nil -> true
      {{h_start, m_start}, {h_end, m_end}}->
        {_, {h_now, m_now, _}} = Util.now
        s = h_start * 60 + m_start
        n = h_now * 60 + m_now
        e = h_end * 60 + m_end
        n >= s && n < e
    end

    chk_interval = case schedule_at do
      nil -> true
      ms ->
        ms_now = Util.ms_now
        ms_interval = Util.to_ms(interval)
        ms_now >= (ms + ms_interval)
    end

    time_able = chk_allow_ats && chk_interval

    if time_able do
      ScheduleLimiter.hold
    else
      false
    end
  end

  def flows_next(state) do
    case flows_out(state.flows) do
      :empty ->
        state_change(state, :finish)
      {flow, flows}->
        state |> start_dispatchers(flow)
              |> Dict.put :flows, flows
    end
  end

  def flows_out([]), do: :empty
  def flows_out([flow | flows]) do
    {flow_pack(flow), flows}
  end

  def flow_pack([]), do: []
  def flow_pack([item | flow]) do
    item = case item do
      {mod, args} when is_list(args) -> {mod, args}
      {mod, arg} -> {mod, [arg]}
      mod -> {mod, []}
    end
    [item | flow_pack(flow)]
  end

end