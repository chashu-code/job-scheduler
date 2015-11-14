defmodule Job.UtilTest  do
  use ExUnit.Case, async: true

  alias Job.Util

  test "ms_now" do
    a = :erlang.system_time :milli_seconds
    b = Util.ms_now

    assert_in_delta a, b, 1
  end

  test "ms_add by default" do
    a = Util.ms_now
    b = Util.ms_add {1, :msecs}

    assert_in_delta a, b, 2
  end

  test "ms_add all unit" do
    a = Util.ms_now

    b = Util.ms_add {2, :msecs}, a
    assert 2 == b - a

    b = Util.ms_add {1, :secs}, a
    assert 1000 == b - a

    b = Util.ms_add {1, :mins}, a
    assert 60 * 1000 == b - a

    b = Util.ms_add {1, :hours}, a
    assert 60 * 60 * 1000 = b - a

    b = Util.ms_add {1, :days}, a
    assert 24 * 60 * 60 * 1000 = b - a
  end
end