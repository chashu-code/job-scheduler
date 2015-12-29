use Mix.Config


# conifg the meter type: :folsom | :none
# default is :none
#
# eg.
# config :job_scheduler,
#   meter: :folsom


# # config the scheduler
# #
#  config :job_scheduler, :scheduler,
#     [name]: %{
#       interval: {number, unit},
#       # execution intervals, unit: :msecs | :secs | :mins | :hours | :days

#       allow_ats: {{hour_start, minute_start}, {hour_end, minute_end}},
#       # allow schedule in time range

#       flows: [
#         [JobMaker],
#          # step 1, will call JobMaker.make()

#         [{JobMaker, :args}, {JobMaker, [:arg1, :arg2]}]
#         # step 2, will call JobMaker.make(args) and JobMaker.make(arg1, arg2) in parallel
#       ]
#       # schedule flows
#     }
