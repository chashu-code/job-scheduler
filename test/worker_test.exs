defmodule Job.WorkerTest  do
  use ExUnit.Case, async: true

  alias Job.Worker

  defmodule M1 do
    def fun0({data, state}), do: {data + 1, state}
    def fun1({data, state}), do: {data, state}
    def fun_err(_job), do: raise "fun_err!"
  end

  defmodule M2 do
    def fun2({data, state}), do: {data, state}
    def fun3_async({data, state}, worker) do
      ms_after = data
      Process.send_after worker, {:pipe_done, {data, state}}, ms_after
    end
  end

  test "parse_pipes" do

    [
      {Job.WorkerTest.M1, :fun0, 0},
      {Job.WorkerTest.M1, :fun1, 0},
      {Job.WorkerTest.M2, :fun2, 0},
      {Job.WorkerTest.M2, :fun3_async, 5000}

    ] = Worker.parse_pipes "Elixir.Job.WorkerTest.M1:fun0 | fun1   | Elixir.Job.WorkerTest.M2:fun2 | fun3_async,5000"
  end

  test "perform async normal" do
    data = 100
    pipe = {Job.WorkerTest.M2, :fun3_async, 200}
    {100, nil} = Worker.perform_pipe {data, nil}, pipe
  end

  test "perform async tiemout" do
    data = 200
    pipe = {Job.WorkerTest.M2, :fun3_async, 100}

    assert_raise Job.Error, ~r/worker pipe timeout/, fn ->
      Worker.perform_pipe {data, nil}, pipe
    end
  end

  test "perform sync" do
    data = 1
    pipe = {Job.WorkerTest.M1, :fun0, 0}
    {2, nil} = Worker.perform_pipe {data, nil}, pipe
  end

  test "handle normal" do
    pipes = Worker.parse_pipes "Elixir.Job.WorkerTest.M1:fun0 | fun1   | Elixir.Job.WorkerTest.M2:fun2 | fun3_async,5000"
    :ok = Worker.handle {1, nil}, pipes
  end

  test "handle timeout" do
    pipes = Worker.parse_pipes "Elixir.Job.WorkerTest.M1:fun0 | fun1   | Elixir.Job.WorkerTest.M2:fun2 | fun3_async,100"
    assert_raise Job.Error, ~r/worker pipe timeout/, fn ->
      Worker.handle {200, nil}, pipes
    end
  end

  test "start and loop normal" do
    job = {1, nil}
    pipes = "Elixir.Job.WorkerTest.M1:fun0 | fun1"
    wrk_pid = spawn_link Worker, :start, [self, pipes]

    assert_receive {:job_next, ^wrk_pid, nil}
    send wrk_pid, {:job, job}
    assert_receive {:job_next, ^wrk_pid, {^job, :ok}}
  end

  test "start and loop error" do
    job = {1, nil}
    pipes = "Elixir.Job.WorkerTest.M1:fun0 | fun_err"
    wrk_pid = spawn_link Worker, :start, [self, pipes]

    assert_receive {:job_next, ^wrk_pid, nil}
    send wrk_pid, {:job, job}
    assert_receive {:job_next, ^wrk_pid,  {^job, %RuntimeError{}}}
  end

end