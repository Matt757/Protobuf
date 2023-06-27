using System;
using System.Collections.Generic;
using System.Threading.Channels;
using Main;

namespace Protobuf
{
    public class NnAtomicRegister : Abstraction
    {
        public Channel<Message> MsgQueue;
        public int N { get; set; }
        public string Key { get; set; }

        public int Timestamp { get; set; }
        public int WriterRank { get; set; }
        public int Value { get; set; }

        public int Acks { get; set; }
        public Value WriteVal { get; set; }
        public int ReadId { get; set; }
        public Dictionary<int, NnarInternalValue> ReadList { get; set; }
        public bool Reading { get; set; }

        public NnAtomicRegister()
        {
            MsgQueue = Channel.CreateUnbounded<Message>();
            ReadList = new Dictionary<int, NnarInternalValue>();
        }

        public void Handle(Message m)
        {
            Console.WriteLine("Nnar is first");
            Message msgToSend = null;
            string aId = GetAbstractionId();

            switch (m.Type)
            {
                case Message.Types.Type.BebDeliver:
                    switch (m.BebDeliver.Message.Type)
                    {
                        case Message.Types.Type.NnarInternalRead:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = aId,
                                ToAbstractionId = aId + ".pl",
                                PlSend = new PlSend
                                {
                                    Destination = m.BebDeliver.Sender,
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.NnarInternalValue,
                                        FromAbstractionId = aId,
                                        ToAbstractionId = aId,
                                        NnarInternalValue = BuildInternalValue(),
                                    }
                                }
                            };
                            break;
                        case Message.Types.Type.NnarInternalWrite:
                            var wMsg = m.BebDeliver.Message.NnarInternalWrite;

                            var vIncoming = new NnarInternalValue { Timestamp = wMsg.Timestamp, WriterRank = wMsg.WriterRank };
                            var vCurrent = new NnarInternalValue { Timestamp = Timestamp, WriterRank = WriterRank };
                            if (Compare(vIncoming, vCurrent) == 1)
                            {
                                Timestamp = wMsg.Timestamp;
                                WriterRank = wMsg.WriterRank;
                                UpdateValue(wMsg.Value);
                            }

                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = aId,
                                ToAbstractionId = aId + ".pl",
                                PlSend = new PlSend
                                {
                                    Destination = m.BebDeliver.Sender,
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.NnarInternalAck,
                                        FromAbstractionId = aId,
                                        ToAbstractionId = aId,
                                        NnarInternalAck = new NnarInternalAck
                                        {
                                            ReadId = ReadId
                                        }
                                    }
                                }
                            };
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                case Message.Types.Type.NnarWrite:
                    ReadId += 1;
                    WriteVal = m.NnarWrite.Value;
                    Acks = 0;
                    ReadList.Clear();

                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.BebBroadcast,
                        FromAbstractionId = aId,
                        ToAbstractionId = aId + ".beb",
                        BebBroadcast = new BebBroadcast
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.NnarInternalRead,
                                FromAbstractionId = aId,
                                ToAbstractionId = aId,
                                NnarInternalRead = new NnarInternalRead
                                {
                                    ReadId = ReadId
                                }
                            }
                        }
                    };
                    break;
                case Message.Types.Type.NnarRead:
                    ReadId += 1;
                    Acks = 0;
                    ReadList.Clear();
                    Reading = true;

                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.BebBroadcast,
                        FromAbstractionId = aId,
                        ToAbstractionId = aId + ".beb",
                        BebBroadcast = new BebBroadcast
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.NnarInternalRead,
                                FromAbstractionId = aId,
                                ToAbstractionId = aId,
                                NnarInternalRead = new NnarInternalRead
                                {
                                    ReadId = ReadId
                                }
                            }
                        }
                    };
                    break;
                case Message.Types.Type.PlDeliver:
                    switch (m.PlDeliver.Message.Type)
                    {
                        case Message.Types.Type.NnarInternalValue:
                            var vMsg = m.PlDeliver.Message.NnarInternalValue;
                            if (vMsg.ReadId == ReadId)
                            {
                                ReadList[m.PlDeliver.Sender.Port] = vMsg;
                                ReadList[m.PlDeliver.Sender.Port].WriterRank = m.PlDeliver.Sender.Rank;

                                if (ReadList.Count > N / 2)
                                {
                                    var h = Highest();
                                    ReadList.Clear();

                                    if (!Reading)
                                    {
                                        h.Timestamp = h.Timestamp + 1;
                                        h.WriterRank = WriterRank;
                                        h.Value = WriteVal;
                                    }

                                    msgToSend = new Message
                                    {
                                        Type = Message.Types.Type.BebBroadcast,
                                        FromAbstractionId = aId,
                                        ToAbstractionId = aId + ".beb",
                                        BebBroadcast = new BebBroadcast
                                        {
                                            Message = new Message
                                            {
                                                Type = Message.Types.Type.NnarInternalWrite,
                                                FromAbstractionId = aId,
                                                ToAbstractionId = aId,
                                                NnarInternalWrite = new NnarInternalWrite
                                                {
                                                    ReadId = ReadId,
                                                    Timestamp = h.Timestamp,
                                                    WriterRank = h.WriterRank,
                                                    Value = h.Value
                                                }
                                            }
                                        }
                                    };
                                }
                            }
                            break;
                        case Message.Types.Type.NnarInternalAck:
                            var ackMsg = m.PlDeliver.Message.NnarInternalAck;
                            if (ackMsg.ReadId == ReadId)
                            {
                                Acks += 1;
                                if (Acks > N / 2)
                                {
                                    Acks = 0;
                                    if (Reading)
                                    {
                                        Reading = false;
                                        msgToSend = new Message
                                        {
                                            Type = Message.Types.Type.NnarReadReturn,
                                            FromAbstractionId = aId,
                                            ToAbstractionId = "app",
                                            NnarReadReturn = new NnarReadReturn
                                            {
                                                Value = BuildInternalValue().Value
                                            }
                                        };
                                    }
                                    else
                                    {
                                        msgToSend = new Message
                                        {
                                            Type = Message.Types.Type.NnarWriteReturn,
                                            FromAbstractionId = aId,
                                            ToAbstractionId = "app",
                                            NnarWriteReturn = new NnarWriteReturn()
                                        };
                                    }
                                }
                            }
                            break;
                        default:
                            throw new Exception("Message not supported");
                    }
                    break;
                default:
                    throw new Exception("Message not supported");
            }

            if (msgToSend != null)
            {
                MsgQueue.Writer.WriteAsync(msgToSend);
            }
        }

        private string GetAbstractionId()
        {
            return "app.nnar[" + Key + "]";
        }

        private NnarInternalValue BuildInternalValue()
        {
            bool defined = (Value != -1);
            return new NnarInternalValue
            {
                ReadId = ReadId,
                Timestamp = Timestamp,
                Value = new Value
                {
                    V = Value,
                    Defined = defined
                }
            };
        }

        private void UpdateValue(Value value)
        {
            if (value.Defined)
            {
                Value = value.V;
            }
            else
            {
                Value = -1;
            }
        }

        private NnarInternalValue Highest()
        {
            NnarInternalValue highest = null;
            foreach (var v in ReadList.Values)
            {
                if (highest == null)
                {
                    highest = v;
                    continue;
                }

                if (Compare(v, highest) == 1)
                {
                    highest = v;
                }
            }

            return highest;
        }

        private int Compare(NnarInternalValue v1, NnarInternalValue v2)
        {
            if (v1.Timestamp > v2.Timestamp)
            {
                return 1;
            }

            if (v2.Timestamp > v1.Timestamp)
            {
                return -1;
            }

            if (v1.WriterRank > v2.WriterRank)
            {
                return 1;
            }

            if (v2.WriterRank > v1.WriterRank)
            {
                return -1;
            }

            return 0;
        }

        public void Destroy()
        {
            // Implement the Destroy method
        }
    }
}
