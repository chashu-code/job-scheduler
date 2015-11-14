require Lager

defmodule Job.WorkerBox do
  use GenServer
  alias Job.Worker


  def start_link({dispatcher, job_info}=state) do
    start_link({dispatcher, job_info, 50})
  end

  def start_link({dispatcher, job_info, allow_down_num}=state) do
    GenServer.start_link(__MODULE__, state, [])
  end

  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  def stop_workers(box) do
    Process.exit box, :shutdown
  end

  def start_worker(box), do: start_workers(box, 1)
  def start_workers(box, num) do
    GenServer.call box, {:start_workers, num}
  end


  def handle_call({:start_workers, num}, _from, {dispatcher, job_info, _}=state) do
    result = make_workers(dispatcher, num, job_info)
    {:reply, result, state}
  end


  def make_workers(dispatcher, 0, _), do: :ok
  def make_workers(dispatcher, num, job_info) do
    # TODO: Node.spawn_link ..
    worker = spawn_link(Worker, :start, [dispatcher, job_info.work_pipes])
    Lager.info "~p job's worker[~p] start_link", [job_info.name, worker]

    make_workers(dispatcher, num - 1, job_info)
  end

  def handle_info({:EXIT, worker, reason},  {dispatcher, job_info, allow_down_num}=state) do
    Lager.error "~p job's worker[~p] exit ~p", [job_info.name, worker, reason]
    send dispatcher, {:worker_down, worker, reason}
    if allow_down_num > 0 do
      {:noreply, {dispatcher, job_info, allow_down_num - 1}}
    else
      raise Job.Error, message: "#{inspect job_info.name} job's worker exit times > allow_down_num"
    end
  end

  # def terminate(reason, {dispatcher, job_info}=state) do
  #   Lager.error "~p job's worker_box terminate. ~p", [job_info.name, reason]
  # end

end