defmodule Job.SchedulerSupTest  do
  use ExUnit.Case, async: true

  alias Job.SchedulerSup
  alias Job.Meter

  defmodule M1 do
    def fun(job), do: job
  end

  defmodule JobMaker do
    def make(name) do
      jobs = [1,2,3]
      work_pipes =  "Elixir.Job.SchedulerSupTest.M1:fun"

      job_info =  %{
        name: name,
        timeout: {5, :msecs},
        work_pipes: work_pipes
      }

      {jobs, job_info}
    end
  end

  def meter_v(key) do
    Meter.get_meter.get_value(key)
  end

  setup_all do
    Application.ensure_all_started :folsom
    Application.put_env :job_scheduler, :meter, :folsom
    Application.put_env :job_scheduler, :scheduler, [
      test1: %{
        flows: [
          [{JobMaker, :test_scheduler_sup1}]
        ]
      },
      test2: %{
        flows: [
          [{JobMaker, :test_scheduler_sup2}]
        ]
      }
    ]
    :ok
  end

  test "start_link" do
    {:ok, spid} = SchedulerSup.start_link
    info = Supervisor.count_children(spid)
    assert info.workers == 2
    assert info.active == 2

    :timer.sleep(100)

    meter_check = fn(n)->
      assert 3 == meter_v [:job, n, :count]
      assert 3 == meter_v [:job, n, :finish]
      assert 0 == meter_v [:job, n, :fail]
      assert 0 == meter_v [:job, n, :retry]
    end

    meter_check.(:test_scheduler_sup1)
    meter_check.(:test_scheduler_sup2)

  end


end