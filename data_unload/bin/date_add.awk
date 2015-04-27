#!/bin/sh

date_add ()
{
  ##函数必须有两个参数
  if [ $# -ne 2 ]
  then
    echo "date_add YYYYMMDD [+|-]DAYS"
    return 1
  fi

  #判断是否为闰年 0-是 1-否
  echo $1,$2|awk -F, '
  function isleap (i_year)
  {
    if (i_year % 400 == 0 || (i_year % 4 == 0 && i_year % 100 != 0))
      return 0;
    else
      return 1;

  }

  ##取月末日期
  function lastday(i_year,i_month)
  {
    if (i_month == 2)
    {
      ret = isleap(i_year)
      if (ret == 0)
        return 29;
      else 
        return 28;
    }
    else if (i_month == 4 || i_month == 6 || i_month == 9 || i_month == 11)
      return 30
    else
      return 31
  }
  {
    i_date=$1;
    i_days=$2;

    #8位日期全为数字
    if (i_date !~/^[0-9][0-9][0-9][0-9][01][0-9][0-3][0-9]$/)
    {
      print "日期参数含有非法字符"
      exit 1
    }

    #加减数不能为0
    if (i_days == 0)
    {
      print "天数参数不能为0"
      exit 1
    }

    #加减数第一位必须是+或-,第二位起必须全为数字
    if (i_days !~/[+-]/ || substr(i_days,2) ~/[^0-9]/)
    {
      print "天数参数不合法"
      exit 1
    }

    v_y = substr(i_date,1,4) + 0
    v_m = substr(i_date,5,2) + 0
    v_d = substr(i_date,7,2) + 0

    #日期参数中月份合法性检查
    if (v_m < 1 || v_m > 12)
    {
      print "月不合法"
      exit 1
    }

    #日期参数中日不能小于1
    if (v_d < 1)
    {
      print "日不合法"
      exit 1
    }

    #日期参数中日不能大于当月末
    v_lastday = lastday(v_y, v_m)
    if (v_d > v_lastday)
    {
      print "日不合法"
      exit 1
    }

    ##开始计算日期
    while (1)
    {
      if (0 == i_days)
        break;

      #日期加
      if (i_days > 0)
      {
        #年底特殊处理
        if (v_m == 12 && v_d == 31)
        {
          v_y++
          v_m = 1
          v_lastday = lastday(v_y, v_m)
          v_d = 1
        }
        else if (v_d == v_lastday) #月末特殊处理
        {
          v_m++
          v_lastday = lastday(v_y, v_m)
          v_d = 1
        }
        else
        {
          v_d++
        }
        i_days--
      }
      else
      {
        #年初特殊处理
        if (v_m == 1 && v_d == 1)
        {
          v_y--
          v_m = 12
          v_lastday = lastday(v_y, v_m)
          v_d = 31
        }
        else if (v_d == 1) #月初特殊处理
        {
          v_m--
          v_lastday = lastday(v_y, v_m)
          v_d = v_lastday
        }
        else
        {
          v_d--
        }
        i_days++
      }
    }
    printf "%d%02d%02d\n", v_y, v_m, v_d
  }'

  return $?
}

#date_add $1 $2
