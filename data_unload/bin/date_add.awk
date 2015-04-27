#!/bin/sh

date_add ()
{
  ##������������������
  if [ $# -ne 2 ]
  then
    echo "date_add YYYYMMDD [+|-]DAYS"
    return 1
  fi

  #�ж��Ƿ�Ϊ���� 0-�� 1-��
  echo $1,$2|awk -F, '
  function isleap (i_year)
  {
    if (i_year % 400 == 0 || (i_year % 4 == 0 && i_year % 100 != 0))
      return 0;
    else
      return 1;

  }

  ##ȡ��ĩ����
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

    #8λ����ȫΪ����
    if (i_date !~/^[0-9][0-9][0-9][0-9][01][0-9][0-3][0-9]$/)
    {
      print "���ڲ������зǷ��ַ�"
      exit 1
    }

    #�Ӽ�������Ϊ0
    if (i_days == 0)
    {
      print "������������Ϊ0"
      exit 1
    }

    #�Ӽ�����һλ������+��-,�ڶ�λ�����ȫΪ����
    if (i_days !~/[+-]/ || substr(i_days,2) ~/[^0-9]/)
    {
      print "�����������Ϸ�"
      exit 1
    }

    v_y = substr(i_date,1,4) + 0
    v_m = substr(i_date,5,2) + 0
    v_d = substr(i_date,7,2) + 0

    #���ڲ������·ݺϷ��Լ��
    if (v_m < 1 || v_m > 12)
    {
      print "�²��Ϸ�"
      exit 1
    }

    #���ڲ������ղ���С��1
    if (v_d < 1)
    {
      print "�ղ��Ϸ�"
      exit 1
    }

    #���ڲ������ղ��ܴ��ڵ���ĩ
    v_lastday = lastday(v_y, v_m)
    if (v_d > v_lastday)
    {
      print "�ղ��Ϸ�"
      exit 1
    }

    ##��ʼ��������
    while (1)
    {
      if (0 == i_days)
        break;

      #���ڼ�
      if (i_days > 0)
      {
        #������⴦��
        if (v_m == 12 && v_d == 31)
        {
          v_y++
          v_m = 1
          v_lastday = lastday(v_y, v_m)
          v_d = 1
        }
        else if (v_d == v_lastday) #��ĩ���⴦��
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
        #������⴦��
        if (v_m == 1 && v_d == 1)
        {
          v_y--
          v_m = 12
          v_lastday = lastday(v_y, v_m)
          v_d = 31
        }
        else if (v_d == 1) #�³����⴦��
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
