using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Main;

namespace Protobuf
{
    public class BestEffortBroadcast : Abstraction
    {
        private readonly Channel<Message> _msgQueue;
        private readonly List<ProcessId> _processes;
        private readonly string _id;

        public BestEffortBroadcast(Channel<Message> msgQueue, List<ProcessId> processes, string id)
        {
            _msgQueue = msgQueue;
            _processes = processes;
            _id = id;
        }

        public async void Handle(Message m)
        {
            Console.WriteLine("AppBeb is first");
            switch (m.Type)
            {
                case Message.Types.Type.BebBroadcast:
                    foreach (var p in _processes)
                    {
                        var msgToSend = new Message
                        {
                            Type = Message.Types.Type.PlSend,
                            FromAbstractionId = _id,
                            ToAbstractionId = $"{_id}.pl",
                            PlSend = new PlSend
                            {
                                Destination = p,
                                Message = m.BebBroadcast.Message
                            }
                        };

                        await _msgQueue.Writer.WriteAsync(msgToSend);
                    }

                    break;
                case Message.Types.Type.PlDeliver:
                    var bebDeliver = new Message
                    {
                        Type = Message.Types.Type.BebDeliver,
                        FromAbstractionId = _id,
                        ToAbstractionId = m.PlDeliver.Message.ToAbstractionId,
                        BebDeliver = new BebDeliver
                        {
                            Sender = m.PlDeliver.Sender,
                            Message = m.PlDeliver.Message
                        }
                    };

                    await _msgQueue.Writer.WriteAsync(bebDeliver);
                    break;
                default:
                    throw new InvalidOperationException("Message not supported");
            }
        }

        public void Destroy()
        {
        }
    }
}
