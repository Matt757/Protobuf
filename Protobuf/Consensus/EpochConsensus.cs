using System.Collections.Generic;
using Protobuf;
using System;
using System.Threading.Channels;
using Main;

namespace Protobuf.Consensus
{

    public class EpochConsensus : Abstraction
    {
        private string id;
        private string parentId;
        private Channel<Message> msgQueue;
        private List<ProcessId> processes;
        private bool aborted;
        private int ets;
        private EpState state;
        private Value tmpVal;
        private Dictionary<string, EpState> states;
        private int accepted;

        public EpochConsensus(string parentAbstraction, string abstractionId, Channel<Message> mQ, List<ProcessId> processes, int ets, EpState state)
        {
            id = abstractionId;
            parentId = parentAbstraction;
            msgQueue = mQ;
            this.processes = processes;
            aborted = false;
            this.ets = ets;
            this.state = state;
            tmpVal = new Value();
            states = new Dictionary<string, EpState>();
            accepted = 0;
        }

        public void Handle(Message m)
        {
            if (aborted)
            {
                return;
            }

            switch (m.Type)
            {
                case Message.Types.Type.EpPropose:
                    tmpVal = m.EpPropose.Value;
                    msgQueue.Writer.WriteAsync(new Message
                    {
                        Type = Message.Types.Type.BebBroadcast,
                        FromAbstractionId = id,
                        ToAbstractionId = id + ".beb",
                        BebBroadcast = new BebBroadcast
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.EpInternalRead,
                                FromAbstractionId = id,
                                ToAbstractionId = id,
                                EpInternalRead = new EpInternalRead()
                            }
                        }
                    });
                    break;
                case Message.Types.Type.PlDeliver:
                    switch (m.PlDeliver.Message.Type)
                    {
                        case Message.Types.Type.EpInternalState:
                            states[Util.GetProcessKey(m.PlDeliver.Sender)] = new EpState
                            {
                                ValTs = m.PlDeliver.Message.EpInternalState.ValueTimestamp,
                                Value = m.PlDeliver.Message.EpInternalState.Value
                            };
                            Console.WriteLine("Number of states: " + states.Count + " vs Number of processes: " + processes.Count);

                            if (states.Count > processes.Count / 2)
                            {
                                EpState hS = Min();
                                if (hS != null && hS.Value != null && hS.Value.Defined)
                                {
                                    Console.WriteLine("this is the val from the process with the highest rank (lowest value):" + tmpVal);
                                    tmpVal = hS.Value;
                                }
                                states = new Dictionary<string, EpState>();

                                msgQueue.Writer.WriteAsync(new Message
                                {
                                    Type = Message.Types.Type.BebBroadcast,
                                    FromAbstractionId = id,
                                    ToAbstractionId = id + ".beb",
                                    BebBroadcast = new BebBroadcast
                                    {
                                        Message = new Message
                                        {
                                            Type = Message.Types.Type.EpInternalWrite,
                                            FromAbstractionId = id,
                                            ToAbstractionId = id,
                                            EpInternalWrite = new EpInternalWrite
                                            {
                                                Value = tmpVal
                                            }
                                        }
                                    }
                                });
                            }
                            break;
                        case Message.Types.Type.EpInternalAccept:
                            accepted++;
                            if (accepted > processes.Count / 2)
                            {
                                accepted = 0;
                                msgQueue.Writer.WriteAsync(new Message
                                {
                                    Type = Message.Types.Type.BebBroadcast,
                                    FromAbstractionId = id,
                                    ToAbstractionId = id + ".beb",
                                    BebBroadcast = new BebBroadcast
                                    {
                                        Message = new Message
                                        {
                                            Type = Message.Types.Type.EpInternalDecided,
                                            FromAbstractionId = id,
                                            ToAbstractionId = id,
                                            EpInternalDecided = new EpInternalDecided
                                            {
                                                Value = tmpVal
                                            }
                                        }
                                    }
                                });
                            }
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                case Message.Types.Type.BebDeliver:
                    switch (m.BebDeliver.Message.Type)
                    {
                        case Message.Types.Type.EpInternalRead:
                            msgQueue.Writer.WriteAsync(new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = id,
                                ToAbstractionId = id + ".pl",
                                PlSend = new PlSend
                                {
                                    Destination = m.BebDeliver.Sender,
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.EpInternalState,
                                        FromAbstractionId = id,
                                        ToAbstractionId = id,
                                        EpInternalState = new EpInternalState
                                        {
                                            ValueTimestamp = state.ValTs,
                                            Value = state.Value
                                        }
                                    }
                                }
                            });
                            break;
                        case Message.Types.Type.EpInternalWrite:
                            state.ValTs = ets;
                            state.Value = m.BebDeliver.Message.EpInternalWrite.Value;
                            msgQueue.Writer.WriteAsync(new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = id,
                                ToAbstractionId = id + ".pl",
                                PlSend = new PlSend
                                {
                                    Destination = m.BebDeliver.Sender,
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.EpInternalAccept,
                                        FromAbstractionId = id,
                                        ToAbstractionId = id,
                                        EpInternalAccept = new EpInternalAccept()
                                    }
                                }
                            });
                            break;
                        case Message.Types.Type.EpInternalDecided:
                            msgQueue.Writer.WriteAsync(new Message
                            {
                                Type = Message.Types.Type.EpDecide,
                                FromAbstractionId = id,
                                ToAbstractionId = parentId,
                                EpDecide = new EpDecide
                                {
                                    Ets = ets,
                                    Value = state.Value
                                }
                            });
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                case Message.Types.Type.EpAbort:
                    msgQueue.Writer.WriteAsync(new Message
                    {
                        Type = Message.Types.Type.EpAborted,
                        FromAbstractionId = id,
                        ToAbstractionId = parentId,
                        EpAborted = new EpAborted
                        {
                            Ets = ets,
                            ValueTimestamp = state.ValTs,
                            Value = state.Value
                        }
                    });
                    aborted = true;
                    break;
                default:
                    throw new Exception("Message not supported");
            }
        }

        private EpState Min()
        {
            EpState state = new EpState();
            foreach (var v in states.Values)
            {
                if (v.ValTs < state.ValTs)
                {
                    state = v;
                }
            }
            return state;
        }
    }
}
