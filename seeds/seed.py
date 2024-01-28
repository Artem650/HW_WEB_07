from random import randint, choice
from faker import Faker
from sqlalchemy.exc import SQLAlchemyError
from conf.db import session
from conf.models import Teacher, Student, Group, Subject, Grade

fake = Faker()

amount_of_students = randint(30, 50)
amount_of_groups = 3
amount_of_subjects = randint(5, 8)
amount_of_teachers = randint(3, 5)
amount_students_in_each_group = int(amount_of_students / amount_of_groups)


def insert_groups():
    for _ in range(amount_of_groups):
        group = Group(
            name=fake.word()
        )
        session.add(group)


def insert_students():
    for _ in range(amount_of_students):
        student = Student(
            fullname=fake.name(),
            group_id=choice(session.query(Group.id).all())[0],
        )
        session.add(student)


def insert_teachers():
    for _ in range(amount_of_teachers):
        teacher = Teacher(
            fullname=fake.name(),
        )
        session.add(teacher)


def insert_subjects():
    for _ in range(amount_of_subjects):
        subject = Subject(
            name=fake.word(),
            teacher_id=choice(session.query(Teacher.id).all())[0],
        )
        session.add(subject)


def insert_grades(student_id, subject_id):
    grade = Grade(
        grade=randint(1, 100),
        grade_date=fake.date_between(start_date='-30d', end_date='today'),
        student_id=student_id,
        subject_id=subject_id,
    )
    session.add(grade)


def grades(students, subjects):
    for student in students:
        for subject in subjects:
            insert_grades(student.id, subject.id)


if __name__ == '__main__':
    if __name__ == '__main__':
        try:
            insert_groups()
            insert_students()
            insert_teachers()
            insert_subjects()
            session.commit()

            students = session.query(Student).all()
            subjects = session.query(Subject).all()
            grades(students, subjects)
            session.commit()
        except SQLAlchemyError as e:
            print(e)
            session.rollback()
        finally:
            session.close()