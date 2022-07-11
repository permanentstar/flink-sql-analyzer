COMPILE AND EXECUTE PLAN
    './test.json' FOR
INSERT INTO t1 SELECT * FROM t2
----------
EXECUTE PLAN './test.json'
----------
COMPILE AND EXECUTE PLAN
'./test.json' FOR
STATEMENT SET BEGIN
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END
----------
COMPILE PLAN './test.json' FOR
INSERT INTO t1 SELECT * FROM t2
----------
COMPILE PLAN './test.json' FOR
STATEMENT SET BEGIN
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END
----------
COMPILE PLAN './test.json' FOR
INSERT INTO t1 SELECT * FROM t2