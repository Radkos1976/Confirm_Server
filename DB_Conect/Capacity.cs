using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DB_Conect
{
    /// <summary>
    /// Get data about CRP in IFS
    /// </summary>
    public class Capacity : Update_pstgr_from_Ora<Capacity.Crp>
    {
        public bool Updated_on_init;
        public List<Crp> Crp_list;
        private readonly Update_pstgr_from_Ora<Crp> rw;
        public Capacity(bool updt)
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Crp>();
                Parallel.Invoke(async () =>
                {
                    if (updt)
                    {                        
                        await Ora_CRP();
                        Updated_on_init = true;
                    }
                    else
                    {
                        Crp_list = await PSTGR_CRP();
                        Crp_list.Sort();
                        Updated_on_init = false;
                    }
                });
            }
            catch (Exception e)
            {
                Loger.Log("Błąd Inicjalizacji obiektu CRP:" + e);
            }
        } 
        public Capacity()
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Crp>();
                Parallel.Invoke(async () =>
                {
                    Crp_list = await PSTGR_CRP();
                    if (Crp_list.Count == 0)
                    {
                        Updated_on_init = true;
                        await Ora_CRP();
                    }
                    else
                    {
                        Crp_list.Sort();
                        Updated_on_init = false;
                    }
                });
            }
            catch (Exception e)
            {
                Loger.Log("Błąd Inicjalizacji obiektu CRP:" + e);
            }
        }
        /// <summary>
        /// Update table with Capacity 
        /// </summary>
        /// <returns></returns>
        public async Task<int> Update_capacity_table()
        {            
            try
            {
                //rw = new Update_pstgr_from_Ora<Crp>();
                List<Crp> list_ora = new List<Crp>();
                List<Crp> list_pstgr = new List<Crp>();
                Parallel.Invoke(
                    async () =>
                {
                    list_ora = await Ora_CRP();
                    list_ora.Sort();
                }, 
                    async () =>
                {
                    list_pstgr = await PSTGR_CRP();
                    list_pstgr.Sort();
                });
                Changes_List<Crp> tmp = rw.Changes(list_pstgr, list_ora, new[] { "id" }, "id", "id");
                list_ora = null;
                list_pstgr = null;
                return await PSTRG_Changes_to_dataTable(tmp, "\"CRP\"", "id", null, null);
            }
            catch (Exception e)
            {
                Loger.Log("Błąd importu CRP:" + e);
                return 1;
            }
        }
        private async Task<List<Crp>> PSTGR_CRP()
        {
            return await rw.Get_PSTGR("Select * from \"CRP\"", "Pstgr_CRP");
        }
        private async Task<List<Crp>> Ora_CRP()
        {
            return await rw.Get_Ora("" +
"select To_Number(a.COUNTER||a.NOTE_ID) id, a.work_day,a.department_no,a.work_center_no,SUM(IFSAPP.Work_Center_Capacity_API.Get_Wc_Capac_Workday__('ST' , a.work_center_no, a.work_day)) capacity," +
"SUM(IFSAPP.Mach_Operation_Load_Util_API.Planned_Load(A.work_day,'ST',a.work_center_no)) planned,SUM(IFSAPP.Mach_Operation_Load_Util_API.Released_Load(A.work_day,'ST',a.work_center_no)) relased," +
"Nvl(Sum(c.godz),0) dop " +
"FROM " +
    "(SELECT a.COUNTER,a.work_day,b.department_no,b.work_center_no,b.NOTE_ID " +
    "FROM " +
         "IFSAPP.work_time_calendar_pub A," +
         "IFSAPP.work_center B " +
     "WHERE A.work_day between SYSDATE-10 and SYSDATE+128 and A.calendar_id = IFSAPP.Work_Center_API.Get_Calendar_Id( B.contract, B.work_center_no ) and B.contract = 'ST' ) a " +
 "left JOIN " +
    "(SELECT  b.dat,ifsapp.work_center_api.Get_Department_No ('ST',b.WORK_CENTER_NO) wydz,b.WORK_CENTER_NO,Sum(b.godz) godz " +
     "from " +
        "(SELECT DOP_ID " +
            "FROM ifsapp.dop_head " +
            "WHERE OBJSTATE IN ('Unreleased','Netted')) a ," +
        "(SELECT DOP_ID,DOP_ORDER_ID,WORK_CENTER_NO,ifsapp.dop_order_api.Get_Revised_Due_Date(DOP_ID,DOP_ORDER_ID) dat, Sum(MACH_RUN_FACTOR) godz " +
            "FROM ifsapp.dop_order_operation " +
            "WHERE ifsapp.dop_head_api.Get_Status(DOP_id) IN ('Unreleased','Netted') " +
            "GROUP BY  DOP_ID,DOP_ORDER_ID,WORK_CENTER_NO,ifsapp.dop_order_api.Get_Revised_Due_Date(DOP_ID,DOP_ORDER_ID)) b " +
     "WHERE b.DOP_ID=a.DOP_ID " +
     "GROUP BY b.dat,ifsapp.work_center_api.Get_Department_No ('ST',b.WORK_CENTER_NO),b.WORK_CENTER_NO ORDER BY dat,wydz,work_center_no) c  " +
"ON c.dat=a.work_day AND C.work_center_no=a.work_center_no " +
"group by a.COUNTER,a.work_day,a.department_no,a.work_center_no,a.NOTE_ID " +
"ORDER BY To_Number(a.COUNTER||a.NOTE_ID)", "ORA_CRP");
        }
        public class Crp : IEquatable<Crp>, IComparable<Crp>
        {
            public long Id { get; set; }
            public DateTime Work_day { get; set; }
            public string Department_no { get; set; }
            public string Work_center_no { get; set; }
            public double Capacity { get; set; }
            public double Planned { get; set; }
            public double Relased { get; set; }
            public double Dop { get; set; }
            /// <summary>
            /// default Comparer by id
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public int CompareTo(Crp other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    return this.Id.CompareTo(other.Id);
                }
            }
            public bool Equals(Crp other)
            {
                if (other == null) return false;
                return (Id.Equals(other.Id));
            }           
        }
    }
}
