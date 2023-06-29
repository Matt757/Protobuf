using System;
using System.Collections.Generic;
using System.Threading.Channels;
using Main;

namespace Protobuf.Consensus
{
    public class EventualLeaderDetector : Abstraction
    {
        private string id;
        private string parentId;
        private Channel<Message> msgQueue;
        private List<ProcessId> processes;
        private Dictionary<string, ProcessId> alive;
        private ProcessId leader;

        public EventualLeaderDetector(string parentAbstraction, string abstractionId, Channel<Message> mQ, List<ProcessId> processes)
        {
            id = abstractionId;
            parentId = parentAbstraction;
            msgQueue = mQ;
            this.processes = processes;
            alive = new Dictionary<string, ProcessId>();
            leader = null;

            foreach (var p in processes)
            {
                alive[Util.GetProcessKey(p)] = p;
            }
        }

        public void Handle(Message m)
        {
            switch (m.Type)
            {
                case Message.Types.Type.EpfdSuspect:
                    string key = Util.GetProcessKey(m.EpfdSuspect.Process);
                    if (alive.ContainsKey(key))
                    {
                        alive.Remove(key);
                    }
                    break;
                case Message.Types.Type.EpfdRestore:
                    alive[Util.GetProcessKey(m.EpfdRestore.Process)] = m.EpfdRestore.Process;
                    break;
                default:
                    throw new Exception("Message not supported");
            }

            UpdateLeader();
        }

        private void UpdateLeader()
        {
            ProcessId max = Util.GetMaxRank(alive);

            if (max == null)
            {
                throw new Exception("Could not determine the process with max rank");
            }

            if (leader == null || Util.GetProcessKey(leader) != Util.GetProcessKey(max))
            {
                leader = max;
                msgQueue.Writer.WriteAsync(new Message
                {
                    Type = Message.Types.Type.EldTrust,
                    FromAbstractionId = id,
                    ToAbstractionId = parentId,
                    EldTrust = new EldTrust
                    {
                        Process = leader
                    }
                });
            }
        }
    }
}
