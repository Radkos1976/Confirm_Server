using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace DB_Conect
{
    /// <summary>
    /// Gets informations about active customer orders
    /// </summary>       
    public class Cust_orders : Update_pstgr_from_Ora<Cust_orders.Orders_row>  
    {
        public bool Updated_on_init;
        public List<Orders_row> Orders_list;
        private Update_pstgr_from_Ora<Orders_row> rw;
        public Cust_orders ()
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Orders_row>();
                Parallel.Invoke(async () =>
                {
                    Orders_list = await Get_PSTGR_List();                   
                    if (Orders_list.Count == 0)
                    {
                        Updated_on_init = true;
                        await Update_cust();
                    }
                    else
                    {
                        Orders_list.Sort();
                        Updated_on_init = false;
                    }
                });
            }
            catch (Exception e)
            {
                Loger.Log("Błąd Inicjalizacji obiektu zamówienia klienta:" + e);
            }
        }
        /// <summary>
        /// Update customer order table
        /// </summary>
        /// <returns></returns>
        public async Task<int> Update_cust()
        { 
            try
            {
                rw = new Update_pstgr_from_Ora<Orders_row>();
                List<Orders_row> list_ora = new List<Orders_row>();
                List<Orders_row> list_pstgr = new List<Orders_row>();
                Parallel.Invoke(async () =>
                {
                    list_ora = await Get_Ora_list(); list_ora.Sort(); Orders_list = list_ora;
                }, async () => { list_pstgr = await Get_PSTGR_List(); list_pstgr.Sort(); });
                Changes_List<Orders_row> tmp = rw.Changes(list_pstgr, list_ora, new[] { "id", "zest", "objversion" }, "Custid", "id");
                list_ora = null;
                list_pstgr = null;
                return await PSTRG_Changes_to_dataTable(tmp, "cust_ord", "id",null,new[] {
                    "update public.cust_ord a " +
                    "SET zest=case when a.dop_connection_db = 'AUT' then " +
                     "case when a.line_state='Aktywowana' then " +
                         "case when dop_made=0 then " +
                            "case when substring(a.part_no,1,1) not in ('5','6','2') " +
                            "then b.zs " +
                            "else null	end " +
                          "else null end " +
                     "else null end else null end " +
                     "from " +
                     "(select ZEST_ID,CASE WHEN zest>1 THEN zest_id ELSE null END as zs " +
                        "from " +
                           "(select a.order_no,a.line_no,b.zest,a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week ZEST_ID " +
                              "from " +
                                 "cust_ord a " +
                                  "left join " +
                                  "(select id,count(zest) zest " +
                                      "from " +
                                         "(select order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week id,part_no zest " +
                                           "from cust_ord " +
                                     "where line_state!='Zarezerwowana' and dop_connection_db='AUT' and seria0=false " +
                                     "and data0 is null group by order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week,part_no ) a " +
                                  "group by id) b " +
                                  "on b.id=a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week " +
                             "where substring(part_no,1,1) not in ('5','6','2') ) a) b " +
                      "where a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week=b.ZEST_ID",
                    "Delete from public.late_ord " +
                         "where cust_id in (SELECT a.cust_id " +
                            "FROM public.late_ord a " +
                                 "left join " +
                                 "public.cust_ord b " +
                                 "on a.cust_id=b.id " +
                               "where b.id is null or b.line_state='Zarezerwowana' or b.dop_qty=b.dop_made)",
                    "Delete from public.cust_ord_history " +
                         "where id in " +
                                "(SELECT a.id FROM " +
                                    "public.cust_ord_history a " +
                                    "left join " +
                                    "public.cust_ord b " +
                                     "on a.id=b.id " +
                                     "where b.id is null)"});
            }
            catch(Exception e)
            {
                Loger.Log("Błąd importu zamówień klienta:" + e);
                return 1;
            }
        }      
        /// <summary>
        /// Get customer orders stored in PSTGR
        /// </summary>
        /// <param name="rw"></param>
        /// <returns></returns>
        private async Task<List<Orders_row>> Get_PSTGR_List()
        {
            return await rw.Get_PSTGR("Select * from cust_ord", "Pstgr_cust_ord");
        }
        /// <summary>
        /// Get present list of customer orders stored in ERP
        /// </summary>
        /// <returns>Present list of orders</returns>
        private async Task<List<Orders_row>> Get_Ora_list()
        {            
            return await rw.Get_Ora("" +
   "SELECT ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO) KOOR,a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO,a.CUSTOMER_PO_LINE_NO," +
   "a.C_DIMENSIONS dimmension,To_Date(c.dat,Decode(InStr(c.dat,'-'),0,'YY/MM/DD','YYYY-MM-DD'))-Delivery_Leadtime Last_Mail_CONF," +
   "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO) STATe_conf,a.STATE LINE_STATE,ifsapp.customer_order_api.Get_State(a.ORDER_NO) CUST_ORDER_STATE," +
   "ifsapp.customer_order_api.Get_Country_Code(a.ORDER_NO) Country,ifsapp.customer_order_api.Get_Customer_No(a.ORDER_NO) CUST_no," +
   "ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO) ZIP_CODE," +
   "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>') ADDR1," +
   "Promised_Delivery_Date-Delivery_Leadtime PROM_DATE,To_Char(Promised_Delivery_Date-Delivery_Leadtime,'IYYYIW') PROM_WEEK,LOAD_ID," +
   "ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID) SHIP_DATE,nvl(a.PART_NO,a.CATALOG_NO) PART_NO," +
   "nvl(ifsapp.inventory_part_api.Get_Description(CONTRACT,a.PART_NO),a.CATALOG_DESC) Descr,a.CONFIGURATION_ID CONFIGURATION,a.BUY_QTY_DUE,a.DESIRED_QTY," +
   "a.QTY_INVOICED,a.QTY_SHIPPED,a.QTY_ASSIGNED,a.DOP_CONNECTION_DB,nvl(b.dop_id,a.Pre_Accounting_Id) dop_id," +
   "ifsapp.dop_head_api.Get_Objstate__(b.dop_id) DOP_STATE,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1),decode(a.DOP_CONNECTION_DB,NULL,a.PLANNED_DUE_DATE)) Data_dop," +
   "b.PEGGED_QTY DOP_QTY," +
   "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0," +
       "Decode(Nvl(LENGTH(TRIM(TRANSLATE(SubStr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id)," +
           "instr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),'-')+1), ' +-.0123456789',' '))),1000),1000,b.PEGGED_QTY,0)),b.QTY_DELIVERED) DOP_MADE," +
   "Nvl(b.CREATE_DATE,decode(a.DOP_CONNECTION_DB,NULL,a.DATE_ENTERED)) DATE_ENTERED," +
   "owa_opt_lock.checksum(a.OBJVERSION||b.OBJVERSION||nvl(b.dop_id,a.Pre_Accounting_Id)||ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO)||c.dat||" +
       "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO)||ifsapp.customer_order_api.Get_State(a.ORDER_NO)||ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO)||" +
       "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||" +
       "ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>')||load_id||ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID)||ifsapp.dop_head_api.Get_Objstate__(b.dop_id)||" +
       "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0,b.PEGGED_QTY),b.QTY_DELIVERED)||" +
       "ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1)) chksum," +
    "a.Pre_Accounting_Id custID,null zest,ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Flag_Db(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Seria0," +
    "ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Date(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Data0,current_timestamp objversion " +
    "FROM " +
       "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO ID,a.* " +
       "from  " +
           "ifsapp.customer_order_line a " +
       "WHERE  a.OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered') ) a " +
       "left JOIN " +
       "ifsapp.dop_demand_cust_ord b " +
       "ON b.ORDER_NO||'_'||b.LINE_NO||'_'||b.REL_NO||'_'||b.LINE_ITEM_NO=a.id " +
       "left JOIN " +
       "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO id," +
       "SubStr(Decode(SubStr(a.MESSAGE_TEXT,-1,1),']',a.MESSAGE_TEXT,a.MESSAGE_TEXT||']'),Decode(InStr(a.message_text,'/',-10,2),0,-11,-9)," +
               "Decode(InStr(a.message_text,'/',-10,2),0,10,8)) DAT " +
       "FROM " +
           "ifsapp.customer_order_line_hist a," +
           "(SELECT Max(HISTORY_NO) hi,a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO  " +
           "FROM " +
               "ifsapp.customer_order_line_hist a," +
               "(SELECT order_no FROM ifsapp.customer_order where OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered'))b  " +
           "WHERE a.order_no=b.order_no AND SubStr(MESSAGE_TEXT,1,3)='Wys'" +
           "GROUP BY a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO) b " +
           "WHERE a.HISTORY_NO=b.HI) c  " +
        "ON c.id=a.id", "ORA_cust_ord");
        }
        public class Orders_row : IEquatable<Orders_row>, IComparable<Orders_row>
        {
            public string Koor { get; set; }
            public string Order_no { get; set; }
            public string Line_no { get; set; }
            public string Rel_no { get; set; }
            public int Line_item_no { get; set; }
            public string Customer_po_line_no { get; set; }
            public double? Dimmension { get; set; }
            public DateTime? Last_mail_conf { get; set; }
            public string State_conf { get; set; }
            public string Line_state { get; set; }
            public string Cust_order_state { get; set; }
            public string Country { get; set; }
            public string Cust_no { get; set; }
            public string Zip_code { get; set; }
            public string Addr1 { get; set; }
            public DateTime Prom_date { get; set; }
            public string Prom_week { get; set; }
            public int? Load_id { get; set; }
            public DateTime? Ship_date { get; set; }
            public string Part_no { get; set; }
            public string Descr { get; set; }
            public string Configuration { get; set; }
            public double Buy_qty_due { get; set; }
            public double Desired_qty { get; set; }
            public double Qty_invoiced { get; set; }
            public double Qty_shipped { get; set; }
            public double Qty_assigned { get; set; }
            public string Dop_connection_db { get; set; }
            public int Dop_id { get; set; }
            public string Dop_state { get; set; }
            public DateTime? Data_dop { get; set; }
            public double Dop_qty { get; set; }
            public double Dop_made { get; set; }
            public DateTime Date_entered { get; set; }
            public int Chksum { get; set; }
            public int Custid { get; set; }
            public string Zest { get; set; }
            public bool? Seria0 { get; set; }
            public DateTime? Data0 { get; set; }
            public Guid Id { get; set; }
            public DateTime Objversion {get; set;}
            /// <summary>
            /// default Comparer by cust_id
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public int CompareTo(Orders_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    return this.Custid.CompareTo(other.Custid);
                }
            }
            public bool Equals(Orders_row other)
            {
                if (other == null) return false;
                return (this.Custid.Equals(other.Custid));
            } 
        }
    } 
}
