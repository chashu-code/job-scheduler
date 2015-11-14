require Lager

defmodule Job.Worker do
  def parse_pipes(pipe_text) when is_binary(pipe_text) do
    pipes = String.split pipe_text, "|"
    parse_pipes pipes, [], %{mod: nil}
  end

  def parse_pipes([], result, _state), do: result |> Enum.reverse
  def parse_pipes([pipe | pipes], result, %{mod: mod}) do
    pipe = case String.split pipe, ":" do
      [mod_new, fun] ->
        mod = mod_new
        make_pipe(mod, fun)
      [fun] -> make_pipe(mod, fun)
    end

    result = [pipe | result]

    parse_pipes(pipes, result, %{mod: mod})
  end

  def make_pipe(mod, fun) do
    mod = mod |> String.strip |> String.to_existing_atom

    {fun, timeout} = case String.split fun, "," do
      [fun] -> {fun, 0}
      [fun, timeout] ->
        timeout = timeout |> String.strip |> String.to_integer
        {fun, timeout}
    end

    fun = fun |> String.strip |> String.to_atom

    args_count = case timeout do
      0 -> 1 # sync
      _ -> 2 # async
    end

    unless function_exported?(mod, fun, args_count) do
      raise Job.Error, message: "[#{mod}.#{fun}/#{args_count}] worker pipe undefined!"
    end

    {mod, fun, timeout}
  end

  def perform_pipe(args, {mod, fun, 0}=pipe) do
    # sync
    result = apply(mod, fun, [args])
  end

  def perform_pipe(args, {mod, fun, timeout}=pipe) do
    # async
    apply(mod, fun, [args, self])
    receive do
      {:pipe_done, result} ->
        result
    after
      timeout ->
        raise Job.Error, message: "[#{inspect pipe}] worker pipe timeout!"
    end
  end

  def handle(result, []), do: :ok
  def handle(args, [pipe | pipes]) do
    result = perform_pipe(args, pipe)
    handle(result, pipes)
  end

  def start(dispatcher, pipes) do
    if is_binary(pipes), do: pipes = parse_pipes(pipes)
    loop(dispatcher, pipes)
  end

  def loop(dispatcher, pipes, prev_job_response \\ nil) do
    send dispatcher, {:job_next, self, prev_job_response}
    receive do
      {:job, job} ->
        try do
          :ok = handle(job, pipes)
          loop(dispatcher, pipes, {job, :ok})
        rescue
          error ->
            Lager.error "worker pipe error ~p\n~p", [error, :erlang.get_stacktrace]
            loop(dispatcher, pipes, {job, error})
        end
    end
  end

end