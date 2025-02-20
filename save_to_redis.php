<?php
require_once 'stk_config.php';
header('Content-Type:application/json;charset=utf-8');
set_time_limit(0);
@ini_set('implicit_flush',1);
ob_implicit_flush(1);

function getRankingIndex($ranking){
    $idxArray   =   array("A1"=>"9","A2"=>"8","A3"=>"7","B1"=>"6","B2"=>"5","B3"=>"4","C1"=>"3","C2"=>"2","C3"=>"1");
    return $idxArray[$ranking];
}

echo date("Y-m-d H:i:s")."---------------syncRemoteData begin---------------\n";
try{
    $redis              =   new Redis();
    $redis->connect(redis_connStr,redis_port);
    $redis->SELECT(4);
    $sysdate            =   $redis->get('rankingSysDate');
    $redismaxDate       =   !empty($sysdate)?$sysdate:'';//获取redis中最新存入时间
    $nowdate            =   my_query("select max(sysdate) as sysdate from pre_stock_statics_hist");
    $sqlmaxdate         =   $nowdate[0]['sysdate'];
    if(false){
        $pbtimeSql      =   "select `stock_id`,`price_date` as pbtime from pre_stock_statics_hist where `dateq_change`=1 group by stock_id,pbtime";
        $sql            =   "select `stock_id`,`price_date` as pdate,ranking from pre_stock_statics_hist group by stock_id,pdate,ranking  order by stock_id,pdate asc";
    }else{
        if($sqlmaxdate  > $redismaxDate){
            $pbtimeSql      =   "select `stock_id`,`price_date` as pbtime from pre_stock_statics_hist where `dateq_change`=1  and  `sysdate`>'".$redismaxDate."' group by stock_id,pbtime";
            $sql            =   "select `stock_id`,`price_date` as pdate,ranking from ";
        }else{
            echo date("Y-m-d H:i:s")."---------------no new data---------------\n";
            echo date("Y-m-d H:i:s")."---------------syncRemoteData end---------------\n";
            exit;
        }
    }
    $pbtimeResult       =   my_query($pbtimeSql);

    if(true){
        $id_price_arr   =   array();
        foreach($pbtimeResult as $val){
            if(!empty($val['pbtime'])){
                $prefix                             =   ($val['stock_id']>600000 && $val['stock_id']<700000)?'SH':'SZ';
                $key                                =   $prefix.'_'.str_pad($val['stock_id'],6,0,STR_PAD_LEFT );
                $id_price_arr[$key]['pbtime'][]     =   $val['pbtime'];
            }
        }
        unset($pbtimeResult);
        $i              =   0;
        while(true){
            $exe_sql    =   '';
            $exe_sql   .= " (select `stock_id`,`price_date`,ranking from pre_stock_statics_hist where id >= ";
            $exe_sql   .= " (select id from pre_stock_statics_hist where id >= ";
            $exe_sql   .= " (select min(id) from pre_stock_statics_hist where `sysdate`>'".$redismaxDate."' ) ";
            $exe_sql   .= " order by id asc limit $i, 1) ";
            $exe_sql   .= " limit 1000) aa group by stock_id,pdate,ranking order by stock_id,pdate asc ";
            $exe_sql    = $sql.$exe_sql;
            $sqlresult  = my_query($exe_sql);
            if(empty($sqlresult)){
                break;
            }
            echo "-----get data form mysql ".count($sqlresult)." itmes ok-----\n";
            foreach($sqlresult as $v){
                if(!empty($v['pdate'])){
                    $prefix                             =   ($v['stock_id']>600000 && $v['stock_id']<700000)?'SH':'SZ';
                    $key                                =   $prefix.'_'.str_pad($v['stock_id'],6,0,STR_PAD_LEFT );
                    $id_price_arr[$key]['date'][]       =   $v['pdate'];
                    $id_price_arr[$key]['ranking'][]    =   intval(!empty($v['ranking'])?getRankingIndex(trim($v['ranking'])):'');
                }
            }
            unset($sqlresult);
            $i+=1000;
        }
        echo date("Y-m-d H:i:s")."---------get id_price_arr ok(".count($id_price_arr).")---------\n";

        foreach($id_price_arr  as $key=>$val){
            $rankingData        =   $redis->hget('ranking',$key);
            $rankingData        =   json_decode($rankingData,true);
            if(isset($id_price_arr[$key]['date'])){
                if(empty($rankingData) || !isset($rankingData['date']) || empty($rankingData['date'])){
                    $rankingData['date']=array();
                    foreach($id_price_arr[$key]['date'] as $dateval){
                        array_push($rankingData['date'],$dateval);
                    }
                    $rankingData['ranking']=array();
                    foreach($id_price_arr[$key]['ranking'] as $rankingval){
                        array_push($rankingData['ranking'],intval($rankingval));
                    }
                }else{
                    foreach($id_price_arr[$key]['date'] as $datepjkey=>$dateval){
                        $old_datepj_key   =   array_search($dateval, $rankingData['date']);
                        if($old_datepj_key !== false){
                            $rankingData['ranking'][$old_datepj_key]=$id_price_arr[$key]['ranking'][$datepjkey];
                        }else{
                            array_push($rankingData['date'],$dateval);
                            array_push($rankingData['ranking'],$id_price_arr[$key]['ranking'][$datepjkey]);
                        }
                    }
                }
            }
            if(isset($id_price_arr[$key]['pbtime'])){
                if(empty($rankingData) || !isset($rankingData['pbtime']) || empty($rankingData['pbtime']) ){
                    $rankingData['pbtime']=array();
                }
                foreach($id_price_arr[$key]['pbtime'] as $pbtimeval){
                    if(!in_array($pbtimeval,$rankingData['pbtime'])){
                        array_push($rankingData['pbtime'],$pbtimeval);
                    }
                }
            }
            $rankingData   =   json_encode($rankingData);
            $redis->hSet('ranking',$key,$rankingData);
            unset($id_price_arr[$key],$rankingData);
        }
        $redis->set('rankingSysDate',$sqlmaxdate);//存入最新评级时抓取的最新时间
        echo date("Y-m-d H:i:s")."---------update data from redis ok---------\n";
    }else{//首次存redis
        $pbtimeArr      =   array();
        foreach($pbtimeResult as $val){
            if(!empty($val['pbtime'])){$pbtimeArr[$val['stock_id']][]  =   $val['pbtime'];}
        }
        unset($pbtimeResult);
        $id_price_arr   =   array();
        $bi             =   0;
        while(true){
            $exeSql     = $sql." limit $bi,1000";
            $sqlresult  = my_query($exeSql);
            if(empty($sqlresult)){
                break;
            }
            echo "-----get data form mysql ".count($sqlresult)." itmes ok-----\n";
            foreach($sqlresult as $v){
                if(!empty($v['pdate'])){
                    $id_price_arr[$v['stock_id']]['date'][]     =   $v['pdate'];
                    $id_price_arr[$v['stock_id']]['ranking'][]  =   intval(!empty($v['ranking'])?getRankingIndex(trim($v['ranking'])):'');
                }
            }
            echo "-----count(id_price_arr):".count($id_price_arr)." ok-----\n";
            unset($sqlresult);
            $bi+=1000;
        }
        foreach($id_price_arr as $key=>$val){
            if(isset($pbtimeArr[$key])){
                $id_price_arr[$key]['pbtime']   =   $pbtimeArr[$key];
            }
        }
        unset($pbtimeArr);
        echo date("Y-m-d H:i:s")."---------get id_price_arr ok---------\n";

        foreach($id_price_arr as $stock_id=>$val){
            $stockid    =   substr(strval(1000000+$stock_id),1);
            $prefix     =   'SZ';
            if($stock_id>600000 && $stock_id<700000){
                $prefix =   'SH';
            }
            $key        =   $prefix.'_'.$stockid;
            $jsonStr    =   json_encode($val);
            $redis->hSet('ranking',$key,$jsonStr);
        }
        $redis->set('rankingSysDate',$sqlmaxdate);//存入最新评级时抓取的最新时间
        echo date("Y-m-d H:i:s")."---------save to redis ok---------\n";
    }
}catch (Exception $e) {
    die($e->getMessage());
}

echo date("Y-m-d H:i:s")."---------------syncRemoteData ok---------------\n";

