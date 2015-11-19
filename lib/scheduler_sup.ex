defmodule Job.SchedulerSup do
  use Supervisor

  def start_link(opts \\ [name: :job_scheduler_sup]) do
    Supervisor.start_link(__MODULE__, [], opts)
  end

  def init(_conf) do
    schedule_ls = Application.get_env :job_scheduler, :scheduler, []

    children = Enum.map schedule_ls, fn ({name, info}) ->
      id = String.to_atom("job_scheduler_#{name}")
      worker(
        Job.Scheduler,
        [ # args
          name, info, [ name: id ]
        ],
        id: id
      )
    end

    supervise(children, strategy: :one_for_one)
  end
end