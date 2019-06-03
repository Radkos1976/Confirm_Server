using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DB_Conect
{
    [Export(typeof(IDbOperations))]
    class DB : IDbOperations
    {
        public void Update_cust_ord()
        {
            int a = 0;
            Get_needs get = new Get_needs();
            Parallel.Invoke(async () => a = await get.Update_cust());           
        }

    }
}
