@0xdc298021a2bf735a;


struct Task {
    id @0 :Text;
    args @2 :List(TaskArg);
    index @3 :Int32;
    max_retries @4 :Int32;
    state :union  {
            submitted :group {

            }
            scheduled :group {
                run_at @0 :Int64;
                cron @1 :Text;
            }
            running :group {

            }
            completed :group {
                result @0 :Data;
                error @1 :Text;
            }
            cancelled :group {

            }
    }
}

struct TaskArg {
        name @0 :Text;
        value @1 :Data;
}

interface Task {
    call @0 (args :List(TaskArg)) -> ();
    cancel @1 () -> ();
    result @2 () -> (result :Data);

}
