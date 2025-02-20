<?php
/**
 * Created by PhpStorm.
 * User: yangJ
 * Date: 2021/3/22 0022
 * Time: 18:02
 */

use util\RedisTool;
require_once dirname(__FILE__)."/commonCheckSid.php";

$result             =   new stdClass();
$result->data       =   "";
$result->success    =   false;

//触发消息时间戳
$dateymd            =   intval(date('Ymd'));
$todayscore         =   strtotime(date('Y-m-d'));
$score              =   isset($_REQUEST['score']) ? $_REQUEST['score'] : 0;
if(!empty($score)&& $score>$todayscore){
    $todayscore     =   $score;
}

//终端
$terminal           =   'APP';
if(isset($_REQUEST['sid'])){
    $terminal       =   'PC';
}else{
    # 自定义范围数据
    $terminal       =   strtoupper($_REQUEST['terminal']) == 'PC'?'PC':'APP';
    $minbegintime   =   strtotime('2024-07-24');//最小起始时间戳
    $todayscore     =   $score<$minbegintime?$minbegintime:$score;
    $dateymd        =   intval(date('Ymd',$todayscore));
}

$_terminal          =   !empty($sid)?PC:MOBILE;
$_terminal_id       =   !empty($sid)?$sid:$pid;
$mid                =   RedisTool::fetchUserIdByHcSoftToken($_terminal_id,$_terminal);
$userchild          =   RedisTool::fechbuysoftbytoken($_terminal_id,['ZNDP','JYJL','AIPJ','JYDP2'],$_terminal);

if(isset($mid) && !empty($mid)){
    $_config = [
        'PC'=>[
            'REDIS'=>[
                ["SOFT:MYMSG:$mid",$dateymd,9999999999],
                ["SOFT:USERALERT:$mid",$dateymd,9999999999],
                ["SOFT:USERALERT_QH:$mid",$dateymd,9999999999],
                ["SOFT:MYMSG_DEAL:$mid",$dateymd,9999999999],
                ["SOFT:MYMSG_STOCKPOOL:$mid",$dateymd,9999999999],
                ["SOFT:JYDP_ALERT:$mid",$dateymd,9999999999],
                ["SOFT:CLDP:$mid",$dateymd,9999999999],
            ],
            'DIC'=>['1'=>'智能盯盘','2'=>'交易精灵','3'=>'价格预警','5'=>'交易明细','6'=>'股票池','12'=>'交易盯盘','13'=>'策略盯盘']
        ],
        'APP'=>[
            'REDIS'=>[
                    ["SOFT:MYMSG:$mid",$dateymd,9999999999],
                    ["SOFT:USERALERT:$mid",$dateymd,9999999999],
                    ["SOFT:USERALERT_QH:$mid",$dateymd,9999999999],
                    ["SOFT:MYMSG_DEAL:$mid",$dateymd,9999999999],
                    ["SOFT:MYMSG_STOCKPOOL:$mid",$dateymd,9999999999],
                    ["SOFT:MYMSG_CIRCLE:$mid",$dateymd,9999999999],
                    ["SOFT:MYMSG_POINT:$mid",$dateymd,9999999999],
                    ["SOFT:CLDP:$mid",$dateymd,9999999999],
                ],
            'DIC'=>['1'=>'智能盯盘','2'=>'交易精灵','3'=>'价格预警','5'=>'交易明细','6'=>'股票池','7'=>'圈子','8'=>'观点','13'=>'策略盯盘']
        ],
        'H5'=>[
            'REDIS'=>[
                    ["SOFT:MYMSG_CIRCLE:$mid",$dateymd,9999999999]
                ],
            'DIC'=>['7'=>'圈子']
        ],
    ];

    $returnarr          =   array();
    $redis              =   RedisTool::getHcyRedis();
    $redis->SELECT(0);

    returnData($returnarr,$_config[$terminal]['REDIS'],$redis,$userchild,$todayscore);

    if(!empty($returnarr)){
        $order_column   =   array_map(function($item){return explode(':',$item)[1];},$returnarr);
        array_multisort($order_column,SORT_ASC, $returnarr);
    }

    $result->dic        =   $_config[$terminal]['DIC'];
    $result->data       =   $returnarr;
    $result->success    =   true;
}

\util\RedisTool::closeall();
echo json_encode($result);
die;

//REDIS - 读取数据
function returnData(&$returnarr,$_config,$redis,$userchild,$todayscore){
    foreach($_config as $redis_config){
        $item               =   explode(':',$redis_config[0]);
        $read_state         =   true;
        if(in_array($item[1],['MYMSG'])){
            $read_state     =   false;
            if($userchild['ZNDP'] || $userchild['JYJL'] || $userchild['AIPJ']){
                $read_state =   true;
            }
        }elseif(in_array($item[1],['JYDP_ALERT'])){
            $read_state     =   false;
            if($userchild['JYDP2']){
                $read_state =   true;
            }
        }
        $msgdatas           =   [];
        if($read_state){
            $msgdatas       =   $redis->zRangeByScore($redis_config[0],$redis_config[1],$redis_config[2]);
        }
        $msgdatas           =   filterData($msgdatas,$todayscore);
        $returnarr          =   array_merge($returnarr,$msgdatas);
    }
}

//过滤数据
function filterData($datas,$todayscore){
    if(!empty($datas)){
        $datas              =   array_filter($datas,function($item) use(&$todayscore){
            $item           =   explode(':',$item);
            return $item[1] > $todayscore;
        });
    }
    return $datas;
}
