defmodule Job.Util do
  @secs 1000
  @mins @secs * 60
  @hours @mins * 60
  @days @hours * 24

  def ms_now do
    :erlang.system_time :milli_seconds
  end

  def to_ms({v, :msecs}), do: v
  def to_ms({v, :secs}), do: v * @secs
  def to_ms({v, :mins}), do: v * @mins
  def to_ms({v, :hours}), do: v * @hours
  def to_ms({v, :days}), do: v * @days

  def ms_add(unit, from \\ ms_now) do
    from + to_ms(unit)
  end

  def now_s(now \\ :calendar.local_time) do
    {{year, month, day},{hour, minute, second}} = now

    date = [year, month, day] |> Enum.map_join("-", &pad/1)
    time = [hour, minute, second] |> Enum.map_join(":", &pad/1)

    date <> " " <> time
  end

  defp pad(num) do
    String.rjust("#{num}", 2, ?0)
  end

  def now, do: :calendar.local_time

end