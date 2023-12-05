# utl-left-join-on-id-and-date-between-start-and-end-in-wps-r-and-python
Left join on id and date between start and end in wps r anf python 
    %let utl-left-join-on-id-and-date-between-start-and-end-in-wps-r-and-python;

    Left join on id and date between start and end in wps r anf python

       Four Solutions
           1. wps sql
           2. wps r sql
           3. wps python sql
           4. wps r native
              https://stackoverflow.com/users/516548/g-grothendieck


    https://stackoverflow.com/questions/77605623/temporal-join-in-r


    /**************************************************************************************************************************/
    /*                                                      |                                 |                               */
    /*                           INPUTS                     |           RULES                 |            OUTPUT             */
    /*                                                      |                                 |                               */
    /*             DATES                     ITEMS          | items.item_id = dates.id        |                               */
    /*                                                      | and items.date                  |                               */
    /*  ID     FRO           TOO       ITEM_ID      DATE    | between dates.fro and dates.too | ITEM_ID  FRO      TOO         */
    /*                              |                       |                                 |                               */
    /*   1   01JAN2022    25JAN2022 |     1       05JAN2022 | 04JAN2022                       |       1  01JAN2022 25JAN2022  */
    /*   1   01MAR2022    17MAR2022 |     1       04MAR2022 | is not between                  |       1  01MAR2022 17MAR2022  */
    /*   2   01MAY2022    30MAY2022 |     2       04JAN2022 |     01MAY2022 30MAY2022         |       2          .         .  */
    /*                                                      |                                 |                               */
    /*                                                      |                                 |                               */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */


    options validvarname=upcase;
    libname sd1 "d:/sd1";

    data sd1.dates;
      informat fro too date9.;
      format   fro too date9.;
      input id fro too;
    cards4;
    1 01JAN2022 25JAN2022
    1 01MAR2022 17MAR2022
    2 01MAY2022 30MAY2022
    ;;;;
    run;quit;

    data sd1.items;
      informat DATE date9.;
      format DATE date9.;
      input item_id date;
    cards4;
    1 05JAN2022
    1 04MAR2022
    2 04JAN2022
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.DATES total obs=3                SD1.ITEMS  total obs=3                                                            */
    /*                                                                                                                        */
    /*    FRO           TOO       ID          DATE       ITEM_ID                                                              */
    /*                                                                                                                        */
    /*  01JAN2022    25JAN2022     1        05JAN2022       1                                                                 */
    /*  01MAR2022    17MAR2022     1        04MAR2022       1                                                                 */
    /*  01MAY2022    30MAY2022     2        04JAN2022       2                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
      libname sd1 "d:/sd1";
      proc sql;
        create
           table sd1.want as
        select
            l.item_id
           ,r.fro
           ,r.too
        from
           sd1.items as l left  join sd1.dates as r
        on
               l.item_id = r.id
           and l.date between r.fro and r.too
      ;quit;
      ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.WANT obs total obs=3                                                                                               */
    /*                                                                                                                        */
    /* Obs    ITEM_ID         FRO           TOO                                                                               */
    /*                                                                                                                        */
    /*  1        1       01JAN2022    25JAN2022                                                                               */
    /*  2        1       01MAR2022    17MAR2022                                                                               */
    /*  3        2               .            .                                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    libname sd1 "d:/sd1";

    %utl_submit_wps64x('
    options validvarname=any;
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.items r=items;
    export data=sd1.dates r=dates;
    submit;
    library(sqldf);
    dates;
    items;
    want<-sqldf("
        select
            l.item_id
           ,r.fro
           ,r.too
        from
           items as l left join dates as r
        on
               l.item_id = r.id
           and l.date between r.fro and r.too
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*              WPS R                                    WPS                                                              */
    /*                                                                                                                        */
    /*    ITEM_ID        FRO        TOO        ITEM_ID          FRO          TOO                                              */
    /*                                                                                                                        */
    /*  1       1 2022-01-01 2022-01-25           1       01JAN2022    25JAN2022                                              */
    /*  2       1 2022-03-01 2022-03-17           1       01MAR2022    17MAR2022                                              */
    /*  3       2       <NA>       <NA>           2               .            .                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;


    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.items python=items;
    export data=sd1.dates python=dates;
    submit;
    import pyreadstat as pr;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
        select
            l.item_id
           ,r.fro
           ,r.too
        from
           items as l left join dates as r
        on
               l.item_id = r.id
           and l.date between r.fro and r.too
    ''');
    print(want);
    pr.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5);
    endsubmit;
    run;quit;
    ");

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;
    data want;
      set xpt.want;
      format froDte tooDte date9.;
      froDte=input(substr(fro,1,10),yymmdd10.);
      tooDte=input(substr(too,1,10),yymmdd10.);
      drop fro too;
    run;quit;
    proc print;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                            WPS PYTHON                                                                                  */
    /*                                                                                                                        */
    /*  Obs    ITEM_ID               FRO                           TOO                                                        */
    /*                                                                                                                        */
    /*   1        1       2022-01-01 00:00:00.000000    2022-01-25 00:00:00.000000                                            */
    /*   2        1       2022-03-01 00:00:00.000000    2022-03-17 00:00:00.000000                                            */
    /*   3        2                                                                                                           */
    /*                                                                                                                        */
    /*                  WPS                                                                                                   */
    /*                                                                                                                        */
    /*    ITEM_ID       FRODTE       TOODTE                                                                                   */
    /*                                                                                                                        */
    /*       1       01JAN2022    25JAN2022                                                                                   */
    /*       1       01MAR2022    17MAR2022                                                                                   */
    /*       2               .            .                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _                                             _   _
    | || |   __      ___ __  ___   _ __   _ __   __ _| |_(_)_   _____
    | || |_  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` | __| \ \ / / _ \
    |__   _|  \ V  V /| |_) \__ \ | |    | | | | (_| | |_| |\ V /  __/
       |_|     \_/\_/ | .__/|___/ |_|    |_| |_|\__,_|\__|_| \_/ \___|
                      |_|
    */

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(dplyr);
    from <- c(as.Date("2022/1/1"), as.Date("2022/3/1"), as.Date("2022/5/1"));
    to <- c(as.Date("2022/1/25"), as.Date("2022/3/17"), as.Date("2022/5/30"));
    id <- c(1,1,2);
    dates <- data.frame(from, to, id);
    item_id <- c(1, 1, 2);
    date <- c(as.Date("2022/1/5"), as.Date("2022/3/4"), as.Date("2022/1/4"));
    items <-data.frame(item_id, date);
    from <- c(as.Date("2022/1/1"), as.Date("2022/3/1"), NA);
    to <- c(as.Date("2022/1/25"), as.Date("2022/3/17"), NA);
    frotoo<- data.frame(from, to);
    result2 <- items %>%
      left_join(dates, join_by(item_id == id, between(date, from, to)));
    result2;
    endsubmit;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS R System                                                                                                      */
    /*                                                                                                                        */
    /*    item_id       date       from         to                                                                            */
    /*  1       1 2022-01-05 2022-01-01 2022-01-25                                                                            */
    /*  2       1 2022-03-04 2022-03-01 2022-03-17                                                                            */
    /*  3       2 2022-01-04       <NA>       <NA>                                                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
