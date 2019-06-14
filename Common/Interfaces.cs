using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.Composition;

namespace Common
{
    public interface IPluginInfo
    {
        string DisplayName { get; }
        string Description { get; }
        string Version { get; }
    }
    public interface IDB_Loger
    {
        void Log(string txt);
        void Srv_start();
        void Srv_stop();
        DateTime Serw_run { get; }
    }    
    public interface IRunnable
    {
       void Run();
    }
    public interface IDbOperations
    {
       void Update_cust_ord();
    } 
}
