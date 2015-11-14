require Lager

defmodule Job.DispatcherTest do
  use ExUnit.Case, async: true

  alias Job.Dispatcher
  alias Job.Meter

  defmodule M1 do
    def fun0(job), do: job
    def fun1(job), do: job+1
    def fun_err(job) do
      if job === 1 do
        raise "fun_err!"
      else
        job
      end
    end
    def fun_exit(job) do
      if job === 1 do
        Process.exit(self, :kill)
      end
      job
    end
    def fun_timeout(job) do
      if rem(job, 2) == 0 do
        :timer.sleep(50)
      end
      job
    end
  end

  defmodule JobMaker do
    def make(name) do

      jobs = case name do
        :dispatch_empty-> nil
        :dispatch_ctrl->
          [2,4,3]
        _-> [1,2,3]
      end

      work_pipes = case name do
        :dispatch_error ->
           "Elixir.Job.DispatcherTest.M1:fun0 | fun_err"
        :dispatch_timeout ->
          "Elixir.Job.DispatcherTest.M1:fun0 | fun_timeout"
        :dispatch_ctrl ->
          "Elixir.Job.DispatcherTest.M1:fun0 | fun_timeout"
        :dispatch_exit ->
          "Elixir.Job.DispatcherTest.M1:fun_exit"
        _ ->
           "Elixir.Job.DispatcherTest.M1:fun0 | fun1"
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

  setup_all do
    Application.ensure_all_started :folsom
    Application.put_env :job_scheduler, :meter, :folsom
    :ok
  end

  test "make empty jobs, direct to finish." do
    {:ok, der} = Dispatcher.start_link
    assert :init == Dispatcher.state(der)

    info = %{
      job_make: {JobMaker, [:dispatch_empty]},
      evt_receiver: self
    }


    Dispatcher.start der, info

    assert_receive {:dispatcher_start, ^der}
    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_empty, :state]
    assert 0 == meter_v [:job, :dispatch_empty, :count]
    assert 0 == meter_v [:job, :dispatch_empty, :finish]
    assert 0 == meter_v [:job, :dispatch_empty, :fail]
    assert 0 == meter_v [:job, :dispatch_empty, :retry]

  end

  test "normal" do
    {:ok, der} = Dispatcher.start_link


    info = %{
      job_make: {JobMaker, [:dispatch_normal]},
      evt_receiver: self
    }

    Dispatcher.start der, info

    assert_receive {:dispatcher_start, ^der}
    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_normal, :state]
    assert 3 == meter_v [:job, :dispatch_normal, :count]
    assert 3 == meter_v [:job, :dispatch_normal, :finish]
    assert 0 == meter_v [:job, :dispatch_normal, :fail]
    assert 0 == meter_v [:job, :dispatch_normal, :retry]
  end

  test "work pipe error" do
    {:ok, der} = Dispatcher.start_link


    info = %{
      job_make: {JobMaker, [:dispatch_error]},
      evt_receiver: self
    }

    Dispatcher.start der, info

    assert_receive {:dispatcher_start, ^der}
    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_error, :state]
    assert 3 == meter_v [:job, :dispatch_error, :count]
    assert 2 == meter_v [:job, :dispatch_error, :finish]
    assert 1 == meter_v [:job, :dispatch_error, :fail]
    assert 3 == meter_v [:job, :dispatch_error, :retry]
  end

  test "job timeout" do
    {:ok, der} = Dispatcher.start_link


    info = %{
      job_make: {JobMaker, [:dispatch_timeout]},
      evt_receiver: self
    }

    Dispatcher.start der, info

    assert_receive {:dispatcher_start, ^der}
    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_timeout, :state]
    assert 3 == meter_v [:job, :dispatch_timeout, :count]
    assert 2 == meter_v [:job, :dispatch_timeout, :finish]
    assert 1 == meter_v [:job, :dispatch_timeout, :fail]
    assert 3 == meter_v [:job, :dispatch_timeout, :retry]
  end

  test "control dispatcher" do
    {:ok, der} = Dispatcher.start_link

    info = %{
      job_make: {JobMaker, [:dispatch_ctrl]},
      evt_receiver: self
    }

    Dispatcher.start der, info
    assert_receive {:dispatcher_start, ^der}

    # ensure
    # worker got job,
    :timer.sleep(5)

    Dispatcher.pause der
    assert_receive {:dispatcher_pause, ^der}
    assert :pause == meter_v [:job, :dispatch_ctrl, :state]

    # sleep 60ms,
    # 2 job timeout and 2 work response
    :timer.sleep(60)

    assert 2 == meter_v [:job, :dispatch_ctrl, :retry]
    assert 0 == meter_v [:job, :dispatch_ctrl, :finish]
    assert 0 == meter_v [:job, :dispatch_ctrl, :fail]

    Dispatcher.restart der
    assert_receive {:dispatcher_restart, ^der}

    # sleep
    # 2 job fail
    :timer.sleep(100)

    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_ctrl, :state]
    assert 3 == meter_v [:job, :dispatch_ctrl, :count]
    assert 1 == meter_v [:job, :dispatch_ctrl, :finish]
    assert 2 == meter_v [:job, :dispatch_ctrl, :fail]
    assert 6 == meter_v [:job, :dispatch_ctrl, :retry]

  end

  test "worker exit, restart new" do
    {:ok, der} = Dispatcher.start_link
    info = %{
      job_make: {JobMaker, [:dispatch_exit]},
      evt_receiver: self
    }

    Dispatcher.start der, info

    assert_receive {:dispatcher_finish, ^der}

    assert :finish == meter_v [:job, :dispatch_exit, :state]
    assert 3 == meter_v [:job, :dispatch_exit, :count]
    assert 2 == meter_v [:job, :dispatch_exit, :finish]
    assert 1 == meter_v [:job, :dispatch_exit, :fail]
    assert 3 == meter_v [:job, :dispatch_exit, :retry]
  end
end