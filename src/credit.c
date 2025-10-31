/*
 * -*-C-*-
 * credit check
 */

 #include <string.h>
 #include <stdio.h>
 
 #include <mysql/mysql.h>
 
 #include "spt_proc.h"
 #include "tpc.h"
 
 extern MYSQL **ctx;
 extern MYSQL_STMT ***stmt;
 
 /*
  * the stock level transaction
  */
 int credit( int t_num,
       int w_id_arg,		/* warehouse id */
       int d_id_arg,		/* district id */
       int c_id_arg		/* customer id */
 )
 {
     int            w_id = w_id_arg;
     int            d_id = d_id_arg;
     int            c_id = c_id_arg;
     float           c_balance;
     int            c_credit_lim;
     float           neworder_balance;
     char            c_credit[4];
 
     int proceed = 0;

     MYSQL_STMT*   mysql_stmt;
         MYSQL_BIND    param[7];
         MYSQL_BIND    column[2];
 
     /*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
     /*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/
 
     /* find the next order id */
 #ifdef DEBUG
     printf("select 1\n");
 #endif
     proceed = 1;
     /*EXEC_SQL SELECT c_balance, c_credit_lim
                     INTO :c_balance, :c_credit_lim
                     FROM customer
                     WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;*/
     mysql_stmt = stmt[t_num][35];
     memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize */
     param[0].buffer_type = MYSQL_TYPE_LONG;
     param[0].buffer = &c_id;
     param[1].buffer_type = MYSQL_TYPE_LONG;
     param[1].buffer = &d_id;
     param[2].buffer_type = MYSQL_TYPE_LONG;
     param[2].buffer = &w_id;
     if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
     if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
 
     if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
     memset(column, 0, sizeof(MYSQL_BIND) * 2); /* initialize */
     column[0].buffer_type = MYSQL_TYPE_FLOAT;
     column[0].buffer = &c_balance;
     column[1].buffer_type = MYSQL_TYPE_LONG;
     column[1].buffer = &c_credit_lim;
     if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
     switch( mysql_stmt_fetch(mysql_stmt) ) {
         case 0: //SUCCESS
         case MYSQL_DATA_TRUNCATED:
         break;
         case 1: //ERROR
         case MYSQL_NO_DATA: //NO MORE DATA
         default:
         mysql_stmt_free_result(mysql_stmt);
         goto sqlerr;
     }
     mysql_stmt_free_result(mysql_stmt);
 
     proceed = 2;
     /*EXEC_SQL SELECT SUM(ol_amount) INTO :neworder_balance
                     FROM order_line, orders, new_order
                     WHERE ol_o_id = o_id AND ol_d_id = :d_id AND ol_w_id = :w_id AND
                        o_d_id = :d_id AND o_w_id = :w_id AND o_c_id = :c_id AND
                        no_o_id = o_id AND no_d_id = :d_id AND no_w_id = :w_id;*/
     mysql_stmt = stmt[t_num][36];

     memset(param, 0, sizeof(MYSQL_BIND) * 7); /* initialize */
     param[0].buffer_type = MYSQL_TYPE_LONG;
     param[0].buffer = &d_id;
     param[1].buffer_type = MYSQL_TYPE_LONG;
     param[1].buffer = &w_id;
     param[2].buffer_type = MYSQL_TYPE_LONG;
     param[2].buffer = &d_id;
     param[3].buffer_type = MYSQL_TYPE_LONG;
     param[3].buffer = &w_id;
     param[4].buffer_type = MYSQL_TYPE_LONG;
     param[4].buffer = &c_id;
     param[5].buffer_type = MYSQL_TYPE_LONG;
     param[5].buffer = &d_id;
     param[6].buffer_type = MYSQL_TYPE_LONG;
     param[6].buffer = &w_id;
     if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
     if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

     if( mysql_stmt_store_result(mysql_stmt) )  goto sqlerr;
             memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize */
             column[0].buffer_type = MYSQL_TYPE_FLOAT;
             column[0].buffer = &neworder_balance;
     if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
             switch( mysql_stmt_fetch(mysql_stmt) ) {
                 case 0: //SUCCESS
                 case MYSQL_DATA_TRUNCATED:
                     break;
                 case 1: //ERROR
                 case MYSQL_NO_DATA: //NO MORE DATA
                 default:
                     mysql_stmt_free_result(mysql_stmt);
                     goto sqlerr;
             }
             mysql_stmt_free_result(mysql_stmt);
 
     if(c_balance + neworder_balance > c_credit_lim) {
        c_credit[0] = 'B';
        c_credit[1] = 'C';
        c_credit[2] = '\0';
     } else {
        c_credit[0] = 'G';
        c_credit[1] = 'C';
        c_credit[2] = '\0';
     }

     proceed = 3;
	/*EXEC_SQL UPDATE customer SET c_credit = :c_credit
		WHERE c_id =:c_id AND c_d_id = :d_id and c_w_id = :w_id;*/
	mysql_stmt = stmt[t_num][37];

	memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize */
    param[0].buffer_type = MYSQL_TYPE_LONG;
    param[0].buffer = &c_credit;
    param[0].buffer_type = MYSQL_TYPE_LONG;
    param[0].buffer = &c_id;
    param[1].buffer_type = MYSQL_TYPE_LONG;
    param[1].buffer = &d_id;
    param[2].buffer_type = MYSQL_TYPE_LONG;
    param[2].buffer = &w_id;
	if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

     /*EXEC_SQL CLOSE ord_line;*/
     /*EXEC_SQL COMMIT WORK;*/
     if( mysql_commit(ctx[t_num]) ) goto sqlerr;
 
     return (1);
 
 sqlerr:
         fprintf(stderr, "credit %d:%d\n",t_num,proceed);
     error(ctx[t_num],mysql_stmt);
    //      /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
    //  /*EXEC_SQL ROLLBACK WORK;*/
     mysql_rollback(ctx[t_num]);
 sqlerrerr:
     return (0);
 
 }
 