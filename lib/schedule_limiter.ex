require Lager

defmodule Job.ScheduleLimiter do
  use GenServer

  alias Job.Util

  def start_link() do
    settings = Application.get_env(:job_scheduler, :settings, [])
    hold_max = Keyword.get settings, :parallel_max, 0
    GenServer.start_link(__MODULE__, %{hold: 0, hold_max: hold_max}, [name: :job_schedule_limiter])
  end

  def init(state) do
    {:ok, state}
  end

  def hold do
    GenServer.call(:job_schedule_limiter, :hold)
  end


  def handle_call(:hold, _from, %{hold: hold, hold_max: hold_max}=state) do
    hold_new = case hold_max do
      0 -> hold + 1
      _ ->
        if hold < hold_max do
          hold + 1
        else
          hold
        end
    end
    state = Dict.put state, :hold, hold_new
    {:reply, hold_new > hold, state}
  end

  def release do
    GenServer.call(:job_schedule_limiter, :release)
  end

  def handle_call(:release, _from, %{hold: hold, hold_max: hold_max}=state) do
    hold_new = max(hold - 1, 0)
    state = Dict.put state, :hold, hold_new
    {:reply, hold > hold_new, state}
  end

end
