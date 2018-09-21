----------------------------- MODULE migration -----------------------------


EXTENDS Naturals, Sequences

VARIABLES Manager, DBs, UsingDbIdx, Status

vars == << Manager, DBs, UsingDbIdx, Status >>

Init == /\ Manager = [status |-> "executive", buf |-> <<>>, migBuf |-> <<>>]
        /\ DBs = <<[buf |-> <<>>, remainingRows |-> <<1>>], [miged |-> <<>>]>>
        /\ UsingDbIdx = 1
        /\ Status = "PROC"

SendSQL(sql) == /\ Status = "PROC"
                /\ Len(Manager.buf) < 5
                /\ Manager.status = "executive"
                /\ Manager' = [status |-> Manager.status, buf |-> Append(Manager.buf, sql), migBuf |-> Manager.migBuf]
                /\ UNCHANGED <<DBs, UsingDbIdx, Status>>

ReqExec ==  /\ Status = "PROC"
            /\ Len(Manager.buf) > 0
            /\ LET sql == Head(Manager.buf)
                IN /\ Manager' = [status |-> Manager.status, buf |-> Tail(Manager.buf), migBuf |-> Manager.migBuf]
                    /\ DBs' = [DBs EXCEPT ![UsingDbIdx] = [
                        buf |-> Append(DBs[UsingDbIdx].buf, sql), 
                        remainingRows |-> DBs[UsingDbIdx].remainingRows
                    ]]
            /\ UNCHANGED <<UsingDbIdx, Status>>

SendMig == /\ Status = "PROC"
                /\ Len(Manager.migBuf) < 5
                /\ Len(DBs[UsingDbIdx].remainingRows) > 0
                /\ LET mig == Head(DBs[UsingDbIdx].remainingRows)
                    IN /\ Manager' = [status |-> Manager.status, buf |-> Manager.buf, migBuf |-> Append(Manager.migBuf, mig)]
                        /\ DBs' = [DBs EXCEPT ![UsingDbIdx] = [
                            buf |-> DBs[UsingDbIdx].buf, 
                            remainingRows |-> Tail(DBs[UsingDbIdx].remainingRows)
                        ]]
                /\ UNCHANGED <<UsingDbIdx, Status>>

MigExec == /\ Status = "PROC"
                /\ Len(Manager.migBuf) > 0
                /\ LET mig == Head(Manager.migBuf)
                    IN /\ Manager' = [status |-> Manager.status, buf |-> Manager.buf, migBuf |-> Tail(Manager.migBuf)]
                        /\ DBs' = [DBs EXCEPT ![2] = [
                            miged |-> Append(DBs[2].miged, mig)
                        ]]
                /\ UNCHANGED <<UsingDbIdx, Status>>

Fin == /\ Status = "PROC"
        /\ Len(DBs[1].remainingRows) = 0
        /\ Len(DBs[2].miged) = 3
        /\ Status' = "FIN"
        /\ UsingDbIdx' = 2
        /\ UNCHANGED <<Manager, DBs>>

Next == \* \/ \E sql \in {"sql"}: SendSQL(sql)
        \/ ReqExec
        \/ SendMig
        \/ MigExec
        \/ Fin
        
Spec == Init /\ [][Next]_vars

Termination == [](Status = "FIN" => UsingDbIdx = 2 /\ Len(DBs[2].miged) = 3)
----

TypeInvariant ==/\ Manager.status \in {"executive"}
                /\ Len(Manager.buf) < 10
                /\ Len(Manager.migBuf) < 10


====
