from sqlalchemy import String, func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT s.group_id, ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    WHERE g.subject_id = 1
    GROUP BY s.group_id
    ORDER BY average_grade DESC;
    """
    result = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).filter(Grade.subject_id == 1).group_by(Student.group_id).order_by(
        desc('average_grade')).all()
    return result


def select_04():
    """
    SELECT ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).all()
    return result


def select_05():
    """
   SELECT
    teachers.fullname AS teacher_name,
    subjects.name AS course_name
FROM
    teachers
JOIN
    subjects ON teachers.id = subjects.teacher_id;
    """
    result = session.query(Teacher.fullname.label('teacher_name'), Subject.name.label('course_name')) \
        .join(Subject, Teacher.id == Subject.teacher_id).all()
    return result


def select_06():
    """
    SELECT g.name AS group_name, string_agg(s.fullname, ', ') AS students_list
    FROM students s
    JOIN groups g ON s.group_id = g.id
    where g.id = 1
    GROUP BY g.name;
    """
    result = session.query(Group.name, func.aggregate_strings(Student.fullname, ',').label('students_list')) \
        .select_from(Student).join(Group).filter(Group.id == 1).group_by(Group.name).all()
    return result


def select_07():
    """
    SELECT s.fullname AS student_name, g.name AS group_name,
    sub.name AS subject_name,
    STRING_AGG(CAST(gr.grade AS TEXT), ', ') AS student_grades
    FROM students s
    JOIN groups g ON s.group_id = g.id
    JOIN grades gr ON s.id = gr.student_id
    JOIN subjects sub ON gr.subject_id = sub.id
    where sub.id = 1
    GROUP BY s.fullname, g.name, sub.name;
    """

    result = session.query(Student.fullname, Group.name, Subject.name,
                           func.aggregate_strings(func.cast(Grade.grade, String), ',').label('student_grades')) \
        .select_from(Student).join(Group).join(Grade).join(Subject).filter(Subject.id == 1).group_by(
        Student.fullname).group_by(Group.name) \
        .group_by(Subject.name).all()
    return result


def select_08():
    """
  SELECT teachers.fullname AS teacher_name, round(AVG(grades.grade),2) AS average_grade
FROM teachers
JOIN subjects ON teachers.id = subjects.teacher_id
JOIN grades ON subjects.id = grades.subject_id
GROUP BY teachers.fullname;
    """
    result = (session.query(Teacher.fullname.label("teacher_name"),
                            func.round(func.avg(Grade.grade), 2).label("average_grade"))
              .join(Subject, Teacher.id == Subject.teacher_id)) \
        .join(Grade, Subject.id == Grade.subject_id).group_by(Teacher.fullname).all()
    return result


def select_09():
    """
    SELECT DISTINCT s.id, s.fullname, subj.id , subj.name
    FROM students s
    JOIN grades m ON s.id = m.student_id
    JOIN subjects subj ON m.subject_id = subj.id
    WHERE s.id = 1;
    """

    result = session.query(func.distinct(Student.id, Student.fullname, Subject.name, Subject.id)) \
        .select_from(Student).join(Grade).join(Subject).filter(Student.id == 1).all()
    return result


def select_10():
    """
    SELECT s.fullname AS student_name, t.fullname AS teacher_name, su.name AS subject_name
FROM students s
JOIN groups g ON s.group_id = g.id
JOIN grades gr ON gr.student_id = s.id
JOIN subjects su ON gr.subject_id = su.id
JOIN teachers t ON su.teacher_id = t.id
WHERE s.id = 454;
    """
    result = (
        session.query(
            Student.fullname.label("student_name"),
            Teacher.fullname.label("teacher_name"),
            Subject.name.label("subject_name")
        )
        .join(Group, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.id == 26)
        .all()
    )
    return result


if __name__ == '__main__':
    print("______________01_________________")
    print(select_01())
    print("______________02_________________")
    print(select_02())
    print("______________03_________________")
    print(select_03())
    print("______________04_________________")
    print(select_04())
    print("______________05_________________")
    print(select_05())
    print("______________06_________________")
    print(select_06())
    print("______________07_________________")
    print(select_07())
    print("______________08_________________")
    print(select_08())
    print("______________09_________________")
    print(select_09())
    print("______________10_________________")
    print(select_10())