using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.Composition;

namespace Common
{
    [InheritedExport(typeof(IDB_Loger))]
    public interface IDB_Loger
    {
        void Log(string txt);
        void Srv_start();
        void Srv_stop();
        DateTime Serw_run { get; }
    }
}
