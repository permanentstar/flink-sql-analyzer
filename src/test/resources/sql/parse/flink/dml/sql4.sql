EXECUTE STATEMENT SET
BEGIN
INSERT INTO students
    SELECT name, age FROM school where type = 'student';
INSERT INTO students
    SELECT name, age FROM school where type = 'student';
END
----------
INSERT INTO a.b.c
SELECT * FROM t
----------
EXECUTE STATEMENT SET BEGIN
INSERT INTO students
    SELECT name, age FROM school where type = 'student';
INSERT INTO students
VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END
----------
EXECUTE STATEMENT
SET BEGIN
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END