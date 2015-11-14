require Lager

defmodule Job.Meter do
  def get_meter() do
    case Application.get_env(:job_scheduler, :meter) do
      :folsom ->
        Job.Meter.Folsom
      _ ->
        Job.Meter.None
    end
  end
end

defmodule Job.Meter.None do
  def get_metrics, do: nil

  def new(_type, _name), do: nil
  def get_value(_name), do: nil

  def update_inc(_name, v \\ 1), do: nil
  def update_dec(_name, v \\ 1), do: nil
  def update_gauge(_name, _v), do: nil
  def update_meter(_name, _v), do: nil
  def update_histogram(_name, _v), do: nil

  def notify(_name, _op, _type), do: nil
end

defmodule Job.Meter.Folsom do
  def get_metrics do
     :folsom_metrics.get_metrics
  end

  def new(:counter, name), do: :folsom_metrics.new_counter(name)
  def new(:histogram, name), do: :folsom_metrics.new_histogram(name)
  def new(:gauge, name), do: :folsom_metrics.new_gauge(name)
  def new(:meter, name), do: :folsom_metrics.new_meter(name)

  def get_value(name) do
     :folsom_metrics.get_metric_value(name)
  end

  def update_inc(name, v \\ 1), do: notify(name, {:inc, v}, :counter)
  def update_dec(name, v \\ 1), do: notify(name, {:dec, v}, :counter)
  def update_gauge(name, v), do: notify(name, v, :gauge)
  def update_meter(name, v), do: notify(name, v, :meter)
  def update_histogram(name, v), do: notify(name, v, :histogram)

  def notify(name, op, type) do
    case :folsom_metrics.notify(name, op) do
      :ok -> :ok
      {:error, name, :nonexistent_metric} ->
        new(type, name)
        :folsom_metrics.notify(name, op)
      error ->
        Lager.error "meter folsom notify error! ~p/~p/~p : ~p", [name, type, op, error]
        error
    end
  end
end