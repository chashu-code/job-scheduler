# JobScheduler

定时任务调度

## 安装

  1. Add job_scheduler to your list of dependencies in `mix.exs`:

        def deps do
          [{:job_scheduler, git: "https://github.com/chashu-code/job-scheduler.git"}]
        end

        def application do
          [applications: [:job_scheduler]]
        end
## 配置

   # 可选
   config :job_scheduler, :settings,
    notifier: {MyNotifier, notify},  # 当 scheduler 状态变更，会调用 MyNotify.notify(scheduler_name, state, msg \\ nil)
    parallel_max: 0          # 同时启动 scheduler 的最大数，默认 0 不限制

   # 配置及启动一个 scheduler
   config :job_scheduler, :scheduler,
      your_scheduler_name: %{

        # 定时配置, unit: :msecs | :secs | :mins | :hours | :days
        interval: {number, unit},

        # 可执行时间范围，
        allow_ats: {{num_hour_start, num_minute_start}, {num_hour_end, num_minute_end}},

        # 要执行的任务流程
        flows: [
          # 步骤1, 将会调用YourJobMaker.make()
          [YourJobMaker],

          # 步骤2,  将会并行调用JobMaker.make(args)、 JobMaker.make(arg1, arg2)
          [{JobMaker, :args}, {JobMaker, [:arg1, :arg2]}]
        ]
      }

## API

  # 定义一个JobMaker

  defmodule FetchJobMaker do
    def make() do
      name = __MODULE__
      jobs = ["http://www.baidu.com", "http://...."]

      work_pipes = [
        "Elixir.FetchJobWorker:fetch",
        "Elixir.PrintJobWorker:print"
      ] |> Enum.join("|")

      job_info = %{
        name: name, # 任务名
        timeout: {5, :secs}, # job最长执行时间
        work_pipes: work_pipes, # job处理方法列表
        retry_max: 3           # job最多重试次数
      }

      {jobs, job_info}
    end

    def finish() do
      # jobs 处理完后，会调用 finish()
      # 和调用 FetchJobMaker.make() 在同一个process
      # 所以可以共用 process dict
    end
  end

  # 定义JobWorker
  defmodule FetchJobWorker do
    def fetch(job) do
      # job => "http://www.baidu.com"

      res = http.fetch job  ... 获取信息
    end
  end

  defmodule PrintJobWorker do
    def print(data) do
      # job => FetchJobWorker.fetch(job) 的结果res
      IO.inspect data
    end
  end

  # 定义 state change notifer
  defmodule MyNotifier do
    def notify(name, state, msg) do
      IO.puts "scheduler #{name}, state: #{state}, msg: #{inspect msg}"
    end
  end





