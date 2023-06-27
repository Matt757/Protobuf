using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Main;
using SystemInventory = System.Collections.Generic.Dictionary<string, Protobuf.System>;

namespace Protobuf
{
    public class Program
    {
        private static void Register(PerfectLink pl, string owner, int index, string hubAddress)
        {
            var hubUri = new Uri("tcp://" + hubAddress);
            var hubHost = hubUri.Host;
            var hubPort = hubUri.Port;

            var message = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = new ProcessId
                    {
                        Host = hubHost,
                        Port = hubPort,
                    },
                    Message = new Message
                    {
                        Type = Message.Types.Type.ProcRegistration,
                        ProcRegistration = new ProcRegistration
                        {
                            Owner = owner,
                            Index = index,
                        },
                    },
                },
            };
            Console.WriteLine("Register: " + message);

            pl.Handle(message);
        }

        public static async Task Main()
        {
            var owner = "abc";
            var hubAddress = "127.0.0.1:5000";
            var ports = new List<int> { 5005, 5006, 5007 };
            var index = 0;

            var host = "127.0.0.1";

            var listeners = new List<TcpListener>();

            foreach (var port in ports)
            {
                var networkMessages = Channel.CreateUnbounded<Message>();
                index++;
                var localPort = port;
                var pl = new PerfectLink(host, port, hubAddress);
            
                new Thread((parameters) =>
                {
                    var args = (Tuple<int, int>)parameters;
                    int indexParam = args.Item1;
                    int portParam = args.Item2;
            
                    Register(pl, owner, indexParam, hubAddress);
                    var listener = Util.Listen(host + ":" + portParam, portParam, (data) =>
                    {
                        try
                        {
                            var message = pl.Parse(data);
                            networkMessages.Writer.TryWrite(message);
                            Console.WriteLine("New message received by " + portParam + ": " + message);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Failed to parse the incoming message: " + ex.Message);
                        }
                    });
            
                    listeners.Add(listener);
                    Console.WriteLine(owner + "-" + indexParam + " listening on port: " + string.Join(", ", portParam));
                }).Start(new Tuple<int, int>(index, localPort));
            
                new Thread((parameters) =>
                {
                    var args = (Tuple<int, int>)parameters;
                    int indexParam = args.Item1;
                    int portParam = args.Item2;
                    var systems = new SystemInventory();
                    while (true)
                    {
                        if (networkMessages.Reader.TryRead(out var message))
                        {
                            switch (message.NetworkMessage.Message.Type)
                            {
                                case Message.Types.Type.ProcDestroySystem:
                                    if (systems.ContainsKey(message.SystemId))
                                    {
                                        var s = systems[message.SystemId];
                                        s.Destroy();
                                        systems.Remove(message.SystemId);
                                    }
            
                                    break;
                                case Message.Types.Type.ProcInitializeSystem:
                                    if (!systems.ContainsKey(message.SystemId))
                                    {
                                        Console.WriteLine(indexParam);
                                        var s = System.CreateSystem(message.NetworkMessage.Message, owner, hubAddress,
                                            indexParam);
                                        s.RegisterAbstractions();
                                        Console.WriteLine("Start event loop for port " + portParam);
                                        s.StartEventLoop();
                                        systems.Add(message.SystemId, s);
                                    }
            
                                    break;
                                default:
                                    if (systems.ContainsKey(message.SystemId))
                                    {
                                        var s = systems[message.SystemId];
                                        s.AddMessage(message);
                                    }
                                    else
                                    {
                                        Console.WriteLine("System " + message.SystemId + " not initialized");
                                    }
            
                                    break;
                            }
                        }
                        else
                        {
                            Thread.Sleep(100); // Wait before trying again
                        }
                    }
                }).Start(new Tuple<int, int>(index, localPort));
            }

            var quitChannel = new ManualResetEventSlim();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                quitChannel.Set();
            };

            quitChannel.Wait();

            foreach (var listener in listeners)
            {
                listener.Stop();
            }
        }
    }
}