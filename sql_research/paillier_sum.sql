-- table name : test
-- mysql> describe test;
-- +-------+------+------+-----+---------+-------+
-- | Field | Type | Null | Key | Default | Extra |
-- +-------+------+------+-----+---------+-------+
-- | id    | int  | YES  |     | NULL    |       |
-- +-------+------+------+-----+---------+-------+
-- 1 row in set (0.01 sec)



SET @sumRes=0;
SET @rownr=0;
SET @nSquare=17;

-- slow
delimiter $$
CREATE PROCEDURE pro_test(IN nSquare int, OUT sumRes int)
BEGIN
DECLARE done BOOLEAN DEFAULT 0;
DECLARE o BIGINT;
DECLARE enc_data CURSOR FOR SELECT id from test;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;
 set sumRes=1;
 OPEN enc_data;
  fetch_loop: LOOP
   FETCH enc_data INTO o;
   IF done THEN LEAVE fetch_loop;
   END IF;
   set sumRes = (sumRes*o)%nSquare;
  END LOOP;
 CLOSE enc_data;
END $$
delimiter ;
call pro_test(@nSquare, @sumRes);
select @sumRes;
drop PROCEDURE pro_test;

-- fast
select sum from ( select @rownr:=@rownr+1 AS rowNumber, cast(@sum := @sum * id % @nSquare as DECIMAL(65,0)) as sum from test
cross join (select @sum := 1) s ) tmp order by rowNumber desc limit 1;