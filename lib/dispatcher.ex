require Lager

defmodule Job.Dispatcher do
  use GenServer

  alias Job.Meter
  alias Job.Util
  alias Job.WorkerBox

  def start_link() do
    state = %{
      state: :init,
      work_map: HashDict.new,
      work_queue:  :queue.new,
      timeout_queue: :queue.new,
      meter: Meter.get_meter
    }
    GenServer.start_link(__MODULE__, state, [])
  end

  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  def state(server) do
    GenServer.call server, :state
  end

  def start(server, %{job_make: {_, _}}=args) do
    send server, {:start, args}
  end
  def restart(server), do: send server, :restart
  def pause(server), do: send server, :pause
  def stop(server), do: send server, :stop


  defp ensure_job_info(maker, %{}=job_info) do
    job_info
    |> Dict.put_new(:name, maker)
    |> Dict.put_new(:timeout, {5, :secs})
    |> Dict.put_new(:retry_max, 3)
    |> Dict.put_new(:work_num, 1)
    |> Dict.put(:maker, maker)
  end

  defp meter_init(state) do
    meter = state.meter
    job_name = state.job_info.name

    meter.new(:gauge, [:job, job_name, :count])
    meter.new(:counter, [:job, job_name, :fail])
    meter.new(:counter, [:job, job_name, :finish])
    meter.new(:counter, [:job, job_name, :retry])
    meter.new(:gauge, [:job, job_name, :state])
    meter.new(:gauge, [:job, job_name, :start_at])
    meter.new(:gauge, [:job, job_name, :end_at])

    meter.update_gauge [:job, job_name, :state], state.state
    meter.update_gauge [:job, job_name, :count], state.job_wait
    meter.update_gauge [:job, job_name, :start_at], Util.now_s

    state
  end

  defp state_change(state, state_next, callback \\ &(&1) ) do
    state_now = state.state

    change_able = case state_next do
      :start when state_now === :init -> true
      :pause when state_now === :start -> true
      :restart when state_now === :pause -> true
      :stop -> true
      :finish -> true
      _ -> false
    end

    if change_able do
      event = "dispatcher_#{state_next}" |> String.to_atom
      if state_next === :restart, do: state_next = :start
      state = state
              |> Dict.put(:state, state_next)
              |> callback.()

      state.meter.update_gauge [:job, state.job_info.name, :state], state_next

      if state.evt_receiver do
        send state.evt_receiver, {event, self}
      end

      Lager.info "~p job's dispatcher state to ~p", [state.job_info.name, state_next]
    end

    state
  end


  def handle_info({:worker_down, worker, reason}, state) do
    state = job_response(state, worker, :worker_down)
    WorkerBox.start_worker state.box
    {:noreply, state}
  end

  def handle_call(:state, _from, state) do
    {:reply, state.state, state}
  end

  def handle_info({:start, %{}=args}, state) do
    state_new = state_change state, :start, fn(state)->

      {job_maker, make_args} = args.job_make

      {jobs, job_info} = apply(job_maker, :make, make_args)

      job_info = ensure_job_info job_maker, job_info

      job_wait = case jobs do
        [_item | _] -> length(jobs)
        _ -> 0
      end

      {:ok, box} = WorkerBox.start_link({self, job_info})

      state = state
              |> Dict.merge(%{
                    jobs: jobs,
                    job_wait: job_wait,
                    job_info: job_info,
                    evt_receiver: Dict.get(args, :evt_receiver),
                    box: box
                  })
              |> meter_init


      if job_wait === 0 do
        # no job, finish
        send self, :finish
      else
        WorkerBox.start_workers box, job_info.work_num

        ms_timeout = job_info.timeout |> Util.to_ms
        :erlang.send_after ms_timeout, self, :job_timeout_check
      end

      state
    end

    {:noreply, state_new}
  end

  def handle_info(:pause, state) do
    state_new = state_change(state, :pause)
    {:noreply, state_new}
  end

  def handle_info(:restart, state) do
    state_new = state_change state, :restart, fn(state)->
      job_next(state)
    end
    {:noreply, state_new}

  end

  def handle_info(:stop, state) do
    state_new = state_change(state, :stop)
    WorkerBox.stop_workers state_new.box
    {:stop, :normal, state_new}
  end

  def handle_info(:finish, state) do
    state_new = state_change state, :finish, fn(state)->
      maker = state.job_info.maker
      if function_exported?(maker, :finish, 0) do
        maker.finish
      end
      state
    end
    WorkerBox.stop_workers state_new.box
    {:stop, :normal, state_new}
  end

  def handle_info({:job_next, worker, prev_job_response}, state) do
    state = state
            |> job_response(worker, prev_job_response)
            |> Dict.put(:work_queue, :queue.in(worker, state.work_queue))

    if state.state === :start do
      state = job_next(state)
    end

    {:noreply, state}
  end

  def handle_info(:job_timeout_check, state) do
    state = state |> job_clean_timeout

    if state.state === :start do
      state = job_next(state)
    end

    ms_timeout = state.job_info.timeout |> Util.to_ms
    :erlang.send_after ms_timeout, self, :job_timeout_check

    {:noreply, state}
  end

  def job_clean_timeout(state) do
    timeout_queue = state.timeout_queue
    case :queue.peek(timeout_queue) do
      :empty -> state
      {:value, {worker, ms_timeout}}->
        if ms_timeout < Util.ms_now do # timeout
            state
            |> Dict.put(:timeout_queue,  :queue.drop(timeout_queue))
            |> job_response(worker, :job_timeout)
            |> job_clean_timeout
        else
          state
        end
    end
  end

  def job_next(%{jobs: []}=state), do: state
  def job_next(%{jobs: [ item | jobs]}=state) do
    {job, retry_count} = case item do
      {_job, _retry_count} -> item
      job -> {job, 0}
    end

    work_queue = state.work_queue

    case :queue.peek work_queue do
      :empty -> state
      {:value, worker}-> # has work, send job
        send worker, {:job, job}

        work_queue = :queue.drop work_queue
        work_map = Dict.put state.work_map, worker, {job, retry_count}

        ms_timeout = Util.ms_add state.job_info.timeout
        timeout_queue = :queue.in {worker, ms_timeout}, state.timeout_queue

        state
        |> Dict.merge(%{
                        jobs: jobs,
                        work_queue: work_queue,
                        work_map: work_map,
                        timeout_queue: timeout_queue
                      })
        |> job_next
    end
  end

  def job_response(state, _worker, nil), do: state
  def job_response(state, worker, reason) when is_atom(reason) do
    case get_in(state, [:work_map, worker]) do
      nil -> state
      {job, _retry_count} ->
        job_response(state, worker, {job, reason})
    end
  end
  def job_response(state, worker, {job, res}) do
    job_info = state.job_info
    case get_in(state, [:work_map, worker]) do
      nil -> state
      {^job, retry_count} ->
        job_wait = state.job_wait

        if res === :ok do # 1 job done
          state.meter.update_inc([:job, job_info.name, :finish])
          job_wait = job_wait - 1
        else # res is error
          if retry_count < job_info.retry_max do
            Lager.error "~p job's worker[~p] perform error, retry!\n>job:~p\n>error:~p", [job_info.name, worker, job, res]
            state.meter.update_inc([:job, job_info.name, :retry])
            # put job in jobs again
            state = Dict.put state, :jobs, [{job, retry_count + 1} | state.jobs]
          else
            Lager.critical "~p job's worker[~p] perform fail!\n>job:~p\n>error:~p", [job_info.name, worker, job, res]
            state.meter.update_inc([:job, job_info.name, :fail])
            job_wait = job_wait - 1 # job fail, job_wait - 1
          end
        end

        if job_wait < 1 do # finish all job
          state.meter.update_gauge [:job, job_info.name, :end_at], Util.now_s
          send self, :finish
        end

        state
        |> Dict.put(:job_wait, job_wait)
        |> Dict.put(:work_map, Dict.delete(state.work_map, worker))
      _ -> # isn't job ! delete worker
        state
        |> Dict.put(:work_map, Dict.delete(state.work_map, worker))
    end
  end

end