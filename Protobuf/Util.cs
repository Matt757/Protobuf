using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using Main;

namespace Protobuf
{
    public static class Util
    {
        public static string GetRegisterId(string abstractionId)
        {
            Regex re = new Regex(@"\[(.*)\]");

            MatchCollection matches = re.Matches(abstractionId);
            string key = matches[0].Groups[1].Value;

            return key;
        }

        public static string GetProcessKey(ProcessId p)
        {
            return p.Owner + Int32ToString(p.Index);
        }

        public static ProcessId GetMaxRank(Dictionary<string, ProcessId> processes)
        {
            ProcessId maxRank = null;

            foreach (var kvp in processes)
            {
                if (maxRank == null || kvp.Value.Rank > maxRank.Rank)
                {
                    maxRank = kvp.Value;
                }
            }

            return maxRank;
        }

        public static ProcessId GetMaxRank(List<ProcessId> processes)
        {
            ProcessId maxRank = null;

            foreach (var process in processes)
            {
                if (maxRank == null || process.Rank > maxRank.Rank)
                {
                    maxRank = process;
                }
            }

            return maxRank;
        }
        
        public static ProcessId GetMaxRankSlice(List<ProcessId> processes)
        {
            ProcessId maxRank = null;

            foreach (var v in processes)
            {
                if (maxRank == null || v.Rank > maxRank.Rank)
                {
                    maxRank = v;
                }
            }

            return maxRank;
        }

        public static string Int32ToString(int value)
        {
            return value.ToString();
        }
        
        public static void Send(string address, byte[] data)
        {
            string[] parts = address.Split(':');
            if (parts.Length != 2)
            {
                Console.WriteLine("Invalid address format");
                return;
            }

            string host = parts[0];
            int port;
            if (!int.TryParse(parts[1], out port))
            {
                Console.WriteLine("Invalid port number");
                return;
            }
            
            using (TcpClient client = new TcpClient())
            {
                client.Connect(host, port);

                byte[] sizeBytes = BitConverter.GetBytes(data.Length);
                Array.Reverse(sizeBytes);

                using (NetworkStream stream = client.GetStream())
                {
                    stream.Write(sizeBytes, 0, sizeBytes.Length);
                    stream.Write(data, 0, data.Length);
                }
            }
        }

        public static TcpListener Listen(string address, Action<byte[]> handler)
        {
            string[] parts = address.Split(':');
            string host = parts[0];
            int port = int.Parse(parts[1]);

            TcpListener listener = new TcpListener(IPAddress.Parse(host), port);
            listener.Start();

            Thread thread = new Thread(() =>
            {
                try
                {
                    while (true)
                    {
                        TcpClient client = listener.AcceptTcpClient();
                        Thread innerThread = new Thread(() =>
                        {
                            using (NetworkStream stream = client.GetStream())
                            {
                                byte[] sizeBuffer = new byte[4];
                                int bytesRead = stream.Read(sizeBuffer, 0, 4);

                                if (bytesRead != 4)
                                {
                                    Console.WriteLine("Failed to read the size of the message");
                                    return;
                                }

                                Array.Reverse(sizeBuffer);
                                int dataSize = BitConverter.ToInt32(sizeBuffer, 0);
                                byte[] dataBuffer = new byte[dataSize];
                                bytesRead = stream.Read(dataBuffer, 0, dataSize);

                                if (bytesRead != dataSize)
                                {
                                    Console.WriteLine("Failed to read the content of the message");
                                    return;
                                }
                                handler(dataBuffer);
                            }
                        });

                        innerThread.Start();
                    }
                }
                finally
                {
                    Console.WriteLine("I stopped");
                }
            });

            thread.Start();
            return listener;
        }
    }
}