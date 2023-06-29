using System.Collections.Generic;
using System.Threading.Tasks;
using Main;

public interface Abstraction
{
    void Handle(Message m);
}