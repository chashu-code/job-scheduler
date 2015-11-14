defmodule Job.SchedulerTest do
  use ExUnit.Case, async: true

  alias Job.Util
  alias Job.Scheduler
  alias Job.Meter


  defmodule M1 do
    def fun(job), do: job
    def fun_err(job) do
      if job === 1 do
        raise "fun_err!"
      else
        job
      end
    end
    def fun_timeout(job) do
      if rem(job, 2) == 0 do
        :timer.sleep(500)
      end
      job
    end
  end

  defmodule JobMaker do
    def make(name) do

      jobs = [1,2,4]

      work_pipes = case name do
        :schedule_error ->
          "Elixir.Job.SchedulerTest.M1:fun | fun_err"
        :schedule_timeout ->
          "Elixir.Job.SchedulerTest.M1:fun | fun_timeout"
        _ -> "Elixir.Job.SchedulerTest.M1:fun"
      end

      job_info =  %{
        name: name,
        timeout: {5, :msecs},
        retry_max: 3,
        work_num: 2,
        work_pipes: work_pipes
      }

      {jobs, job_info}
    end
  end

  def meter_v(key) do
    Meter.get_meter.get_value(key)
  end

  def name_add(name, add) do
    "#{name}_#{add}" |> String.to_atom
  end

  setup_all do
    Application.ensure_all_started :folsom
    Application.put_env :job_scheduler, :meter, :folsom
    :ok
  end


  test "schedule_able?" do
    ### allow_ats nil
    allow_ats = nil
    interval = {10, :msecs}

    # schedule_at nil
    schedule_at = nil
    assert true === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval <= now
    schedule_at = Util.ms_now - 10
    assert true === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval > now
    schedule_at = Util.ms_now - 5
    assert false === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    ### now in allow_ats
    {_, {hour, minute, _}} = Util.now
    allow_ats = {{hour, minute},{hour, minute + 1}}

    # schedule_at nil
    schedule_at = nil
    assert true === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval <= now
    schedule_at = Util.ms_now - 10
    assert true === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval > now
    schedule_at = Util.ms_now - 5
    assert false === Scheduler.schedule_able?(schedule_at, interval, allow_ats)


    ### now out allow_ats
    {_, {hour, minute, _}} = Util.now
    allow_ats = {{hour+1, minute},{hour+2, minute}}

    # schedule_at nil
    schedule_at = nil
    assert false === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval <= now
    schedule_at = Util.ms_now - 10
    assert false === Scheduler.schedule_able?(schedule_at, interval, allow_ats)

    # schedule_at + interval > now
    schedule_at = Util.ms_now - 5
    assert false === Scheduler.schedule_able?(schedule_at, interval, allow_ats)
  end

  test "flow_out" do
    flows = []
    :empty = Scheduler.flows_out(flows)

    flows = [ [1], [{2,:arg},{3, []}] ]
    {[{1,[]}], flows} = Scheduler.flows_out(flows)
    {[{2,[:arg]},{3, []}], flows} = Scheduler.flows_out(flows)
    :empty = Scheduler.flows_out(flows)
  end



  test "schedule normal" do
    name = :schedule_normal
    n1 = name_add(name, 1)
    n2 = name_add(name, 2)
    n3 = name_add(name, 3)

    info = %{
      flows: [
        [{Job.SchedulerTest.JobMaker, n1}],
        [{Job.SchedulerTest.JobMaker, n2}, {Job.SchedulerTest.JobMaker, n3}]
      ]
    }

    {:ok, spid} = Scheduler.start_link(name, info)

    :timer.sleep(100)

    meter_check = fn(n)->
      assert 3 == meter_v [:job, n, :count]
      assert 3 == meter_v [:job, n, :finish]
      assert 0 == meter_v [:job, n, :fail]
      assert 0 == meter_v [:job, n, :retry]
    end

    meter_check.(n1)
    meter_check.(n2)
    meter_check.(n3)
  end

  test "schedule work pipe error" do
    name = :schedule_error
    n1 = name
    n2 = name_add(name, 2)

    info = %{
      flows: [
        [{Job.SchedulerTest.JobMaker, n2}, {Job.SchedulerTest.JobMaker, n1}]
      ]
    }

    {:ok, spid} = Scheduler.start_link(name, info)

    :timer.sleep(100)

    assert 3 == meter_v [:job, n1, :count]
    assert 2 == meter_v [:job, n1, :finish]
    assert 1 == meter_v [:job, n1, :fail]
    assert 3 == meter_v [:job, n1, :retry]


    assert 3 == meter_v [:job, n2, :count]
    assert 3 == meter_v [:job, n2, :finish]
    assert 0 == meter_v [:job, n2, :fail]
    assert 0 == meter_v [:job, n2, :retry]
  end

  test "schedule timeout" do
    Process.flag(:trap_exit, true)

    name = :schedule_timeout
    n1 = name
    n2 = name_add(name, 2)

    info = %{
      interval: {80, :msecs},
      flows: [
        [{Job.SchedulerTest.JobMaker, n2}, {Job.SchedulerTest.JobMaker, n1}]
      ]
    }

    {:ok, spid} = Scheduler.start_link(name, info)

    :timer.sleep(100)

    assert_receive {:EXIT, spid, {%Job.Error{}=error, _}}

    assert error.message =~ "scheduler timeout"

    assert 3 == meter_v [:job, n2, :count]
    assert 3 == meter_v [:job, n2, :finish]
    assert 0 == meter_v [:job, n2, :fail]
    assert 0 == meter_v [:job, n2, :retry]
  end

end
