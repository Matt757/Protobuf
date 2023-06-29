using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Main;

namespace Protobuf.Consensus
{
    public class EpochChange : Abstraction
    {
        private string id;
        private string parentId;
        private ProcessId self;
        private Channel<Message> msgQueue;
        private List<ProcessId> processes;
        private ProcessId trusted;
        private int lastTs;
        private int ts;

        public EpochChange(string parentAbstraction, string abstractionId, ProcessId ownProcess, Channel<Message> mQ, List<ProcessId> processes)
        {
            id = abstractionId;
            parentId = parentAbstraction;
            self = ownProcess;
            this.processes = processes;
            msgQueue = mQ;
            trusted = Util.GetMaxRankSlice(processes);
            lastTs = 0;
            ts = ownProcess.Rank;
        }

        public void Handle(Message m)
        {
            switch (m.Type)
            {
                case Message.Types.Type.EldTrust:
                    trusted = m.EldTrust.Process;
                    HandleSelfTrusted(m);
                    break;
                case Message.Types.Type.PlDeliver:
                    switch (m.PlDeliver.Message.Type)
                    {
                        case Message.Types.Type.EcInternalNack:
                            HandleNack();
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                case Message.Types.Type.BebDeliver:
                    switch (m.BebDeliver.Message.Type)
                    {
                        case Message.Types.Type.EcInternalNewEpoch:
                            int newTs = m.BebDeliver.Message.EcInternalNewEpoch.Timestamp;
                            string l = Util.GetProcessKey(m.BebDeliver.Sender);
                            string trustedKey = Util.GetProcessKey(trusted);
                            if (l == trustedKey && newTs > lastTs)
                            {
                                lastTs = newTs;
                                msgQueue.Writer.WriteAsync(new Message
                                {
                                    Type = Message.Types.Type.EcStartEpoch,
                                    FromAbstractionId = id,
                                    ToAbstractionId = parentId,
                                    EcStartEpoch = new EcStartEpoch
                                    {
                                        NewTimestamp = newTs,
                                        NewLeader = m.BebDeliver.Sender
                                    }
                                });
                            }
                            else
                            {
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
                                            Type = Message.Types.Type.EcInternalNack,
                                            FromAbstractionId = id,
                                            ToAbstractionId = id,
                                            EcInternalNack = new EcInternalNack()
                                        }
                                    }
                                });
                            }
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                default:
                    throw new Exception("Message not supported");
            }
        }

        private void HandleNack()
        {
            if (Util.GetProcessKey(self) == Util.GetProcessKey(trusted))
            {
                ts += processes.Count;
                msgQueue.Writer.WriteAsync(new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    FromAbstractionId = id,
                    ToAbstractionId = id + ".beb",
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EcInternalNewEpoch,
                            FromAbstractionId = id,
                            ToAbstractionId = id,
                            EcInternalNewEpoch = new EcInternalNewEpoch
                            {
                                Timestamp = ts
                            }
                        }
                    }
                });
            }
        }

        private void HandleSelfTrusted(Message m)
        {
            trusted = m.EldTrust.Process;
            if (Util.GetProcessKey(self) == Util.GetProcessKey(trusted))
            {
                ts += processes.Count;
                msgQueue.Writer.WriteAsync(new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    FromAbstractionId = id,
                    ToAbstractionId = id + ".beb",
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EcInternalNewEpoch,
                            FromAbstractionId = id,
                            ToAbstractionId = id,
                            EcInternalNewEpoch = new EcInternalNewEpoch
                            {
                                Timestamp = ts
                            }
                        }
                    }
                });
            }
        }
    }
}
