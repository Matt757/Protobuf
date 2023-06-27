using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using Main;

namespace Protobuf
{
    public class App : Abstraction
    {
        private readonly Channel<Message> _msgQueue;

        public App(Channel<Message> msgQueue)
        {
            _msgQueue = msgQueue;
        }

        public async void Handle(Message m)
        {
            Console.WriteLine("App is first");
            Message msgToSend;
            switch (m.Type)
            {
                case Message.Types.Type.PlDeliver:
                    switch (m.PlDeliver.Message.Type)
                    {
                        case Message.Types.Type.AppBroadcast:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.BebBroadcast,
                                FromAbstractionId = "app",
                                ToAbstractionId = "app.beb",
                                BebBroadcast = new BebBroadcast
                                {
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.AppValue,
                                        FromAbstractionId = "app",
                                        ToAbstractionId = "app",
                                        AppValue = new AppValue
                                        {
                                            Value = m.PlDeliver.Message.AppBroadcast.Value
                                        }
                                    }
                                }
                            };
                            break;
                        case Message.Types.Type.AppValue:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.PlSend,
                                FromAbstractionId = "app",
                                ToAbstractionId = "app.pl",
                                PlSend = new PlSend
                                {
                                    Message = new Message
                                    {
                                        Type = Message.Types.Type.AppValue,
                                        AppValue = m.PlDeliver.Message.AppValue
                                    }
                                }
                            };
                            break;
                        case Message.Types.Type.AppWrite:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.NnarWrite,
                                FromAbstractionId = "app",
                                ToAbstractionId = $"app.nnar[{m.PlDeliver.Message.AppWrite.Register}]",
                                NnarWrite = new NnarWrite()
                                {
                                    Value = m.PlDeliver.Message.AppWrite.Value
                                }
                            };
                            break;
                        case Message.Types.Type.AppRead:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.NnarRead,
                                FromAbstractionId = "app",
                                ToAbstractionId = $"app.nnar[{m.PlDeliver.Message.AppRead.Register}]",
                                NnarRead = new NnarRead()
                            };
                            break;
                        case Message.Types.Type.AppPropose:
                            msgToSend = new Message
                            {
                                Type = Message.Types.Type.UcPropose,
                                FromAbstractionId = "app",
                                ToAbstractionId = $"app.uc[{m.PlDeliver.Message.AppPropose.Topic}]",
                                UcPropose = new UcPropose()
                                {
                                    Value = m.PlDeliver.Message.AppPropose.Value
                                }
                            };
                            break;
                        default:
                            throw new InvalidOperationException("Message not supported");
                    }
                    break;
                case Message.Types.Type.BebDeliver:
                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.PlSend,
                        FromAbstractionId = "app",
                        ToAbstractionId = "app.pl",
                        PlSend = new PlSend
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.AppValue,
                                AppValue = m.BebDeliver.Message.AppValue
                            }
                        }
                    };
                    break;
                case Message.Types.Type.NnarWriteReturn:
                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.PlSend,
                        FromAbstractionId = "app",
                        ToAbstractionId = "app.pl",
                        PlSend = new PlSend
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.AppWriteReturn,
                                AppWriteReturn = new AppWriteReturn
                                {
                                    Register = Util.GetRegisterId(m.FromAbstractionId)
                                }
                            }
                        }
                    };
                    break;
                case Message.Types.Type.NnarReadReturn:
                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.PlSend,
                        FromAbstractionId = "app",
                        ToAbstractionId = "app.pl",
                        PlSend = new PlSend
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.AppReadReturn,
                                AppReadReturn = new AppReadReturn
                                {
                                    Register = Util.GetRegisterId(m.FromAbstractionId),
                                    Value = m.NnarReadReturn.Value
                                }
                            }
                        }
                    };
                    break;
                case Message.Types.Type.UcDecide:
                    msgToSend = new Message
                    {
                        Type = Message.Types.Type.PlSend,
                        FromAbstractionId = "app",
                        ToAbstractionId = "app.pl",
                        PlSend = new PlSend
                        {
                            Message = new Message
                            {
                                Type = Message.Types.Type.AppDecide,
                                ToAbstractionId = "app",
                                AppDecide = new AppDecide
                                {
                                    Value = m.UcDecide.Value
                                }
                            }
                        }
                    };
                    break;
                default:
                    throw new InvalidOperationException("Message not supported");
            }
            await _msgQueue.Writer.WriteAsync(msgToSend);
        }

        public void Destroy()
        {
        }
    }
}
