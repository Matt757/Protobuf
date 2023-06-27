using System;
using System.Collections.Generic;
using System.Threading.Channels;
using Google.Protobuf;
using Main;

namespace Protobuf
{
    public class PerfectLink : Abstraction
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _hubAddress;
        private Channel<Message> _msgQueue;
        private string _systemId;
        private string _parentId;
        private List<ProcessId> _processes;

        public PerfectLink(string host, int port, string hubAddress)
        {
            _host = host;
            _port = port;
            _hubAddress = hubAddress;
        }

        public PerfectLink WithProps(string systemId, Channel<Message> msgQueue, List<ProcessId> ps)
        {
            _systemId = systemId;
            _msgQueue = msgQueue;
            _processes = ps;

            return this;
        }

        public PerfectLink CopyWithParentId(string parentAbstraction)
        {
            var newPl = (PerfectLink)MemberwiseClone();
            newPl._parentId = parentAbstraction;

            return newPl;
        }

        public async void Handle(Message m)
        {
            switch (m.Type)
            {
                case Message.Types.Type.NetworkMessage:
                    ProcessId sender = null;
                    foreach (var p in _processes)
                    {
                        if (p.Host == m.NetworkMessage.SenderHost && p.Port == m.NetworkMessage.SenderListeningPort)
                        {
                            sender = p;
                            break;
                        }
                    }

                    var msg = new Message
                    {
                        SystemId = (m.SystemId == null) ? "" : m.SystemId,
                        FromAbstractionId = m.ToAbstractionId,
                        ToAbstractionId = _parentId,
                        Type = Message.Types.Type.PlDeliver,
                        PlDeliver = new PlDeliver
                        {
                            Sender = sender,
                            Message = m.NetworkMessage.Message
                        }
                    };

                    await _msgQueue.Writer.WriteAsync(msg);
                    break;
                case Message.Types.Type.PlSend:
                    Send(m);
                    break;
                default:
                    throw new InvalidOperationException("Message not supported");
            }
        }

        private void Send(Message m)
        {
            Console.WriteLine("Port " + _port + " is sending message " + m);
            var mToSend = new Message
            {
                SystemId = (_systemId == null) ? "" : _systemId,
                ToAbstractionId = m.ToAbstractionId,
                Type = Message.Types.Type.NetworkMessage,
                NetworkMessage = new NetworkMessage
                {
                    Message = m.PlSend.Message,
                    SenderHost = _host,
                    SenderListeningPort = _port
                }
            };

            var data = mToSend.ToByteArray();

            var address = _hubAddress;
            if (m.PlSend.Destination != null)
            {
                address = $"{m.PlSend.Destination.Host}:{m.PlSend.Destination.Port}";
            }
            Console.WriteLine("I send message to: " + address);

            Util.Send(address, data);
        }

        public Message Parse(byte[] data)
        {
            return Message.Parser.ParseFrom(data);
        }

        public void Destroy()
        {
        }
    }
}
