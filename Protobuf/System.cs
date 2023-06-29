using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Main;
using Protobuf.Consensus;

namespace Protobuf
{
    public class System
    {
        private string systemId;
        private Channel<Message> msgQueue;
        private Dictionary<string, Abstraction> abstractions;
        private string hubAddress;
        private ProcessId ownProcess;
        private List<ProcessId> processes;

        public System(Message message, string hubAddress, ProcessId ownProcess)
        {
            systemId = message.SystemId;
            msgQueue = Channel.CreateUnbounded<Message>();
            abstractions = new Dictionary<string, Abstraction>();
            this.hubAddress = hubAddress;
            this.ownProcess = ownProcess;
            processes = message.ProcInitializeSystem.Processes.ToList();
        }

        public static System CreateSystem(Message m, string owner, string hubAddress, int index)
        {
            ProcessId ownProcess = null;
            foreach (var p in m.ProcInitializeSystem.Processes)
            {
                if (p.Owner == owner && p.Index == index)
                {
                    ownProcess = p;
                    break;
                }
            }

            return new System(m, hubAddress, ownProcess);
        }
        
        public void StartEventLoop()
        {
            Thread thread = new Thread(Run);
            thread.IsBackground = true;
            thread.Start();
        }

        private async void Run()
        {
            while (await msgQueue.Reader.WaitToReadAsync())
            {
                while (msgQueue.Reader.TryRead(out var m))
                {
                    if (!abstractions.TryGetValue(m.ToAbstractionId, out var handler))
                    {
                        if (m.ToAbstractionId.StartsWith("app.nnar"))
                        {
                            Console.WriteLine($"Registering abstractions for {m.ToAbstractionId}");
                            RegisterNnarAbstractions(Util.GetRegisterId(m.ToAbstractionId));
                        }
                        if (m.Type == Message.Types.Type.UcPropose)
                        {
                            Console.WriteLine($"Registering abstractions for {m.ToAbstractionId}");
                            RegisterConsensusAbstractions(Util.GetRegisterId(m.ToAbstractionId));
                        }
                    }

                    if (!abstractions.TryGetValue(m.ToAbstractionId, out handler))
                    {
                        Console.WriteLine($"No handler defined for {m.ToAbstractionId}");
                        continue;
                    }

                    Console.WriteLine($"[{m.ToAbstractionId}] handling message {m.Type}");
                    try
                    {
                        handler.Handle(m);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to handle the message: {ex.Message}");
                    }
                }
            }
            Console.WriteLine("I exit after processing all messages");
        }

        public void RegisterAbstractions()
        {
            var pl = new PerfectLink(ownProcess.Host, ownProcess.Port, hubAddress)
                .WithProps(systemId, msgQueue, processes);

            abstractions["app"] = new App(msgQueue);
            abstractions["app.pl"] = pl.CopyWithParentId("app");

            abstractions["app.beb"] = new BestEffortBroadcast(msgQueue, processes, "app.beb");
            abstractions["app.beb.pl"] = pl.CopyWithParentId("app.beb");
        }

        public void Destroy()
        {
            Console.WriteLine($"Destroying system {systemId}");
        }

        public void AddMessage(Message m)
        {
            Console.WriteLine($"Received message for {m.ToAbstractionId} with type {m.Type}");
            msgQueue.Writer.WriteAsync(m);
        }

        public void RegisterNnarAbstractions(string key)
        {
            var pl = new PerfectLink(ownProcess.Host, ownProcess.Port, hubAddress)
                .WithProps(systemId, msgQueue, processes);
            var aId = $"app.nnar[{key}]";

            abstractions[aId] = new NnAtomicRegister
            {
                MsgQueue = msgQueue,
                N = processes.Count,
                Key = key,
                Timestamp = 0,
                WriterRank = ownProcess.Rank,
                Value = -1,
                ReadList = new Dictionary<int, NnarInternalValue>()
            };
            abstractions[aId + ".pl"] = pl.CopyWithParentId(aId);
            abstractions[aId + ".beb"] = new BestEffortBroadcast(msgQueue, processes, aId + ".beb");
            abstractions[aId + ".beb.pl"] = pl.CopyWithParentId(aId + ".beb");
        }

        public void RegisterConsensusAbstractions(string topic)
        {
            var pl = new PerfectLink(ownProcess.Host, ownProcess.Port, hubAddress)
                .WithProps(systemId, msgQueue, processes);
            var aId = $"app.uc[{topic}]";

            abstractions[aId] = new UniformConsensus(aId, msgQueue, abstractions, processes, ownProcess, pl);
            abstractions[aId + ".ec"] = new EpochChange(aId, aId + ".ec", ownProcess, msgQueue, processes);
            abstractions[aId + ".ec.pl"] = pl.CopyWithParentId(aId + ".ec");
            abstractions[aId + ".ec.beb"] = new BestEffortBroadcast(msgQueue, processes, aId + ".ec.beb");
            abstractions[aId + ".ec.beb.pl"] = pl.CopyWithParentId(aId + ".ec.beb");
            abstractions[aId + ".ec.eld"] = new EventualLeaderDetector(aId + ".ec", aId + ".ec.eld", msgQueue, processes);
            abstractions[aId + ".ec.eld.epfd"] = new EventuallyPerfectFailureDetector(aId + ".ec.eld", aId + ".ec.eld.epfd", msgQueue, processes);
            abstractions[aId + ".ec.eld.epfd.pl"] = pl.CopyWithParentId(aId + ".ec.eld.epfd");
        }
    }

    
}
