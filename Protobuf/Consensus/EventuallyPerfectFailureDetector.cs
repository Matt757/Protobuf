using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Main;

namespace Protobuf.Consensus
{
    public class EventuallyPerfectFailureDetector : Abstraction
    {
        private readonly string id;
        private readonly string parentId;
        private readonly Channel<Message> msgQueue;
        private readonly List<ProcessId> processes;

        private Dictionary<string, ProcessId> alive;
        private Dictionary<string, ProcessId> suspected;
        private TimeSpan delay;
        private Timer timer;

        private static readonly TimeSpan delta = TimeSpan.FromMilliseconds(100);

        public EventuallyPerfectFailureDetector(string parentAbstraction, string abstractionId, Channel<Message> mQ, List<ProcessId> processes)
        {
            id = abstractionId;
            parentId = parentAbstraction;
            msgQueue = mQ;
            this.processes = processes;

            alive = new Dictionary<string, ProcessId>();
            suspected = new Dictionary<string, ProcessId>();
            delay = delta;

            // Set all processes as alive
            foreach (var p in processes)
            {
                alive[Util.GetProcessKey(p)] = p;
            }

            StartTimer(delay);
        }

        private void StartTimer(TimeSpan delay)
        {
            timer = new Timer(TimerCallback, null, delay, Timeout.InfiniteTimeSpan);
        }

        private void TimerCallback(object state)
        {
            var msgToSend = new Message
            {
                Type = Message.Types.Type.EpfdTimeout,
                FromAbstractionId = id,
                ToAbstractionId = id,
                EpfdTimeout = new EpfdTimeout()
            };

            msgQueue.Writer.WriteAsync(msgToSend);
        }

        public void Handle(Message m)
        {
            switch (m.Type)
            {
                case Message.Types.Type.EpfdTimeout:
                    HandleTimeout();
                    break;
                case Message.Types.Type.PlDeliver:
                    switch (m.PlDeliver.Message.Type)
                    {
                        case Message.Types.Type.EpfdInternalHeartbeatRequest:
                            msgQueue.Writer.WriteAsync(new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = id,
                                ToAbstractionId = id + ".pl",
                                PlSend = new PlSend
                                {
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.EpfdInternalHeartbeatReply,
                                        FromAbstractionId = id,
                                        ToAbstractionId = id,
                                        EpfdInternalHeartbeatReply = new EpfdInternalHeartbeatReply()
                                    }
                                }
                            });
                            break;
                        case Message.Types.Type.EpfdInternalHeartbeatReply:
                            alive[Util.GetProcessKey(m.PlDeliver.Sender)] = m.PlDeliver.Sender;
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                default:
                    throw new Exception("Message not supported");
            }
        }

        private void HandleTimeout()
        {
            foreach (var k in suspected.Keys)
            {
                if (alive.ContainsKey(k))
                {
                    delay += delta;
                    Console.WriteLine("Increased timeout to " + delay);
                    break;
                }
            }

            foreach (var p in processes)
            {
                var key = Util.GetProcessKey(p);
                var isAlive = alive.ContainsKey(key);
                var isSuspected = suspected.ContainsKey(key);

                if (!isAlive && !isSuspected)
                {
                    suspected[key] = p;

                    msgQueue.Writer.WriteAsync(new Message
                    {
                        Type = Message.Types.Type.EpfdSuspect,
                        FromAbstractionId = id,
                        ToAbstractionId = parentId,
                        EpfdSuspect = new EpfdSuspect
                        {
                            Process = p
                        }
                    });
                }
                else if (isAlive && isSuspected)
                {
                    suspected.Remove(key);
                    msgQueue.Writer.WriteAsync(new Message
                    {
                        Type = Message.Types.Type.EpfdRestore,
                        FromAbstractionId = id,
                        ToAbstractionId = parentId,
                        EpfdRestore = new EpfdRestore
                        {
                            Process = p
                        }
                    });
                }

                // Send heartbeat request
                msgQueue.Writer.WriteAsync(new Message
                {
                    Type = Message.Types.Type.PlSend,
                    FromAbstractionId = id,
                    ToAbstractionId = id + ".pl",
                    PlSend = new PlSend
                    {
                        Destination = p,
                        Message = new Message
                        {
                            Type = Message.Types.Type.EpfdInternalHeartbeatRequest,
                            FromAbstractionId = id,
                            ToAbstractionId = id,
                            EpfdInternalHeartbeatRequest = new EpfdInternalHeartbeatRequest()
                        }
                    }
                });
            }

            alive = new Dictionary<string, ProcessId>();
            StartTimer(delay);
        }

        public void Destroy()
        {
            timer.Dispose();
        }
    }
}
