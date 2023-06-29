using System;
using System.Collections.Generic;
using System.Threading.Channels;
using Main;

namespace Protobuf.Consensus
{
    public class UniformConsensus : Abstraction
    {
        private readonly string id;
        private readonly Channel<Message> msgQueue;
        private readonly Dictionary<string, Abstraction> abstractions;
        private readonly List<ProcessId> processes;
        private readonly ProcessId self;
        private readonly PerfectLink pl;

        private Value val;
        private bool proposed;
        private bool decided;
        private int ets;
        private ProcessId l;
        private int newTs;
        private ProcessId newL;

        public UniformConsensus(string id, Channel<Message> mQ, Dictionary<string, Abstraction> abstractions,
            List<ProcessId> processes, ProcessId ownProcess, PerfectLink pl)
        {
            this.id = id;
            msgQueue = mQ;
            this.abstractions = abstractions;
            this.processes = processes;
            self = ownProcess;
            this.pl = pl;

            val = new Value();
            proposed = false;
            decided = false;
            ets = 0;
            l = Util.GetMaxRankSlice(processes);
            newTs = 0;
            newL = new ProcessId();

            AddEpAbstraction(new EpState());
        }

        private void AddEpAbstraction(EpState state)
        {
            var aId = id + GetEpId();
            abstractions[aId] = new EpochConsensus(id, aId, msgQueue, processes, ets, state);
            abstractions[aId + ".beb"] = new BestEffortBroadcast(msgQueue, processes, aId + ".beb");
            abstractions[aId + ".pl"] = pl.CopyWithParentId(aId);
            abstractions[aId + ".beb.pl"] = pl.CopyWithParentId(aId + ".beb");
            Console.WriteLine("I added the EpochConsensus to the abstractions");
        }

        private string GetEpId()
        {
            return ".ep[" + ets + "]";
        }

        public void Handle(Message m)
        {
            switch (m.Type)
            {
                case Message.Types.Type.UcPropose:
                    val = m.UcPropose.Value;
                    Console.WriteLine("Received message to propose a value: " + m);
                    Console.WriteLine("Proposed value: " + val);
                    break;
                case Message.Types.Type.EcStartEpoch:
                    newTs = m.EcStartEpoch.NewTimestamp;
                    newL = m.EcStartEpoch.NewLeader;
                    msgQueue.Writer.WriteAsync(new Message
                    {
                        Type = Message.Types.Type.EpAbort,
                        FromAbstractionId = id,
                        ToAbstractionId = id + GetEpId(),
                        EpAbort = new EpAbort()
                    });
                    break;
                case Message.Types.Type.EpAborted:
                    if (ets == m.EpAborted.Ets)
                    {
                        ets = newTs;
                        l = newL;
                        proposed = false;
                        AddEpAbstraction(new EpState
                        {
                            ValTs = ets,
                            Value = m.EpAborted.Value
                        });
                    }
                    break;
                case Message.Types.Type.EpDecide:
                    if (ets == m.EpDecide.Ets)
                    {
                        if (!decided)
                        {
                            decided = true;
                            msgQueue.Writer.WriteAsync(new Message
                            {
                                Type = Message.Types.Type.UcDecide,
                                FromAbstractionId = id,
                                ToAbstractionId = "app",
                                UcDecide = new UcDecide
                                {
                                    Value = m.EpDecide.Value
                                }
                            });
                        }
                    }
                    break;
                default:
                    throw new Exception("Message not supported");
            }

            UpdateLeader();
        }

        private void UpdateLeader()
        {
            if (Util.GetProcessKey(l) == Util.GetProcessKey(self) &&
                val.Defined &&
                !proposed)
            {
                proposed = true;
                msgQueue.Writer.WriteAsync(new Message
                {
                    Type = Message.Types.Type.EpPropose,
                    FromAbstractionId = id,
                    ToAbstractionId = id + GetEpId(),
                    EpPropose = new EpPropose
                    {
                        Value = val
                    }
                });
            }
        }
    }
}
