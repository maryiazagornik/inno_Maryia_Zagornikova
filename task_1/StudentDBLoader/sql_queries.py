"""SQL queries used in the StudentDBLoader application."""

# Table creation
CREATE_TABLES = """
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS rooms CASCADE;

CREATE TABLE rooms
(
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE students
(
    id       INT PRIMARY KEY,
    name     VARCHAR(255) NOT NULL,
    birthday DATE         NOT NULL,
    sex      CHAR(1)      NOT NULL,
    room_id  INT,
    CONSTRAINT fk_room
        FOREIGN KEY (room_id)
            REFERENCES rooms (id)
            ON DELETE SET NULL
);
"""

# Indexes
CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room_id);
CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);
CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);
"""

# Business queries
QUERIES = {
    "room_student_count": """
    SELECT r.name, COUNT(s.id) AS student_count
    FROM rooms r
    LEFT JOIN students s ON r.id = s.room_id
    GROUP BY r.id, r.name
    ORDER BY student_count DESC;
    """,
    "top5_min_avg_age": """
    SELECT r.name,
           AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) AS avg_age
    FROM rooms r
    JOIN students s ON r.id = s.room_id
    GROUP BY r.id, r.name
    HAVING COUNT(s.id) > 0
    ORDER BY avg_age ASC LIMIT 5;
    """,
    "top5_max_age_diff": """
    SELECT r.name,
           MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) -
           MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) AS age_difference
    FROM rooms r
    JOIN students s ON r.id = s.room_id
    GROUP BY r.id, r.name
    HAVING COUNT(s.id) > 1
    ORDER BY age_difference DESC LIMIT 5;
    """,
    "mixed_gender_rooms": """
    SELECT r.name
    FROM rooms r
    JOIN students s ON r.id = s.room_id
    GROUP BY r.id, r.name
    HAVING COUNT(DISTINCT s.sex) > 1;
    """
}
