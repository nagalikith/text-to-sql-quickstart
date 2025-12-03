import os
import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
import hashlib
import secrets

fake = Faker()

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="canvas_development",
    user="postgres",
    password="sekret",
    host="localhost",
    port=5433
)
cur = conn.cursor()

# Configuration
NUM_USERS = 50
NUM_TERMS = 3
NUM_COURSES_PER_TERM = 10
NUM_ENROLLMENTS_PER_USER = 3
ROOT_ACCOUNT_ID = 1
role_map = {
    "StudentEnrollment": 3,
    "TeacherEnrollment": 4,
    "TaEnrollment": 5,
    "ObserverEnrollment": 7,
}

# Password hashing setup
password = "password123" 
password_salt = secrets.token_hex(16)
crypted_password = hashlib.sha512(f"password123{password_salt}".encode()).hexdigest()
persistence_token = secrets.token_hex(32)
single_access_token = secrets.token_hex(16)
perishable_token = secrets.token_hex(16)
reset_password_token = secrets.token_hex(16)

# --- Generate Users with Login Credentials ---
user_ids = []
for _ in range(NUM_USERS):
    created_at = fake.date_time_between(start_date="-2y", end_date="now")
    workflow_state = "active"
    name = fake.name()
    first_name = name.split()[0] if name.split() else fake.first_name()
    last_name = name.split()[-1] if len(name.split()) > 1 else fake.last_name()
    email = fake.email()
    sortable_name = f"{last_name}, {first_name}"
    username = email.split('@')[0] + str(random.randint(100, 999))
    
    # Insert into users
    cur.execute("""
        INSERT INTO users (workflow_state, created_at, updated_at, root_account_ids, 
                          name, short_name, sortable_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, [workflow_state, created_at, created_at, [ROOT_ACCOUNT_ID], 
          name, first_name, sortable_name])
    user_id = cur.fetchone()[0]
    user_ids.append(user_id)
    unique_id_normalized = username.lower()  # Normalize to lowercase
    
    # Insert into pseudonyms (login credentials)
    cur.execute("""
    INSERT INTO pseudonyms (
        user_id, account_id, workflow_state, unique_id, 
        unique_id_normalized,
        crypted_password, password_salt, 
        persistence_token, single_access_token, perishable_token,
        login_count, failed_login_count,
        created_at, updated_at, sis_user_id,
        reset_password_token
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", [user_id, ROOT_ACCOUNT_ID, "active", username, 
      unique_id_normalized,
      crypted_password, password_salt,
      persistence_token, single_access_token, perishable_token,
      0, 0,  # login_count, failed_login_count
      created_at, created_at, f"SIS_{user_id}",
      reset_password_token])

# --- Generate Terms ---
term_ids = []
for _ in range(NUM_TERMS):
    t = fake.date_time_between(start_date="-1y", end_date="now")
    name = f"Fall {t.year}"
    start_at = t
    end_at = t + timedelta(days=120)
    cur.execute("""
        INSERT INTO enrollment_terms (root_account_id, created_at, name, start_at, updated_at)
        VALUES (%s,%s,%s,%s,%s) RETURNING id
    """, [ROOT_ACCOUNT_ID, t, name, start_at, end_at])
    term_ids.append(cur.fetchone()[0])

# --- Generate Courses ---
course_ids = []
for term_id in term_ids:
    for _ in range(NUM_COURSES_PER_TERM):
        created_at = fake.date_time_between(start_date="-1y", end_date="now")
        name = f"{fake.word().title()} {fake.word().title()} {random.randint(100, 999)}"
        course_code = f"{fake.random_uppercase_letter()}{fake.random_uppercase_letter()}{random.randint(100, 999)}"
        
        # Assign a teacher
        teacher_id = random.choice(user_ids)
        
        cur.execute("""
            INSERT INTO courses (
                account_id, root_account_id, enrollment_term_id, created_at, updated_at,
                name, course_code, workflow_state, is_public
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, [ROOT_ACCOUNT_ID, ROOT_ACCOUNT_ID, term_id, created_at, created_at, 
              name, course_code, "available", False])
        
        course_id = cur.fetchone()[0]
        course_ids.append(course_id)

# --- Generate Course Sections ---
section_ids_by_course = {}
course_default_section_ids = {}  # Track default section for each course

for course_id in course_ids:
    # Create 1-3 sections per course
    num_sections = random.randint(1, 3)
    section_ids = []
    
    for i in range(num_sections):
        created_at = fake.date_time_between(start_date="-1y", end_date="now")
        section_name = f"Section {fake.random_uppercase_letter()}" if i == 0 else f"Section {i+1}"
        section_sis_id = f"SECT_{course_id}_{random.randint(100,999)}"
        
        cur.execute("""
            INSERT INTO course_sections (
                course_id, name, root_account_id, 
                created_at, updated_at, sis_source_id,
                workflow_state, default_section, accepting_enrollments
            )
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s)
            RETURNING id
        """, [
            course_id, section_name, ROOT_ACCOUNT_ID,
            created_at, created_at, section_sis_id,
            "active", (i == 0), True  # First section is default
        ])
        
        section_id = cur.fetchone()[0]
        section_ids.append(section_id)
        
        if i == 0:  # Store the default section ID
            course_default_section_ids[course_id] = section_id
    
    section_ids_by_course[course_id] = section_ids

# --- Generate Teacher Enrollments (AFTER sections are created) ---
# Get teacher for each course and enroll them
for course_id in course_ids:
    # Get the default section for this course
    default_section_id = course_default_section_ids.get(course_id)
    
    if not default_section_id:
        # Fallback to first section if no default found
        default_section_id = section_ids_by_course[course_id][0]
    
    # Find or assign a teacher for this course
    # (We need to track which user is teacher for each course)
    # For simplicity, we'll assign a random teacher
    teacher_id = random.choice(user_ids)
    
    created_at = fake.date_time_between(start_date="-1y", end_date="now")
    
    # Check if this teacher is already enrolled in this course
    cur.execute("""
        SELECT COUNT(*) FROM enrollments 
        WHERE user_id = %s AND course_id = %s
    """, [teacher_id, course_id])
    
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO enrollments (
                user_id, course_id, type, workflow_state, created_at, updated_at,
                course_section_id, root_account_id, limit_privileges_to_course_section, role_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            teacher_id, course_id, "TeacherEnrollment", "active",
            created_at, created_at, default_section_id, ROOT_ACCOUNT_ID, False, role_map["TeacherEnrollment"]
        ])

# --- Generate Student Enrollments ---
for user_id in user_ids:
    # Each user enrolls in 1-5 courses
    num_enrollments = random.randint(1, min(5, len(course_ids)))
    enrolled_courses = random.sample(course_ids, num_enrollments)
    
    for course_id in enrolled_courses:
        # Skip if user is already teacher of this course
        cur.execute("""
            SELECT COUNT(*) FROM enrollments 
            WHERE user_id = %s AND course_id = %s AND type = 'TeacherEnrollment'
        """, [user_id, course_id])
        
        if cur.fetchone()[0] > 0:
            continue  # Skip, user is already a teacher in this course
        
        # Choose a section for this enrollment
        section_ids = section_ids_by_course[course_id]
        section_id = random.choice(section_ids)
        
        created_at = fake.date_time_between(start_date="-1y", end_date="now")
        enrollment_type = "StudentEnrollment"
        
        cur.execute("""
            INSERT INTO enrollments (
                user_id, course_id, type, workflow_state, created_at, updated_at,
                course_section_id, root_account_id, limit_privileges_to_course_section, role_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            user_id, course_id, enrollment_type, "active",
            created_at, created_at, section_id, ROOT_ACCOUNT_ID, False, role_map[enrollment_type]
        ])

# --- Add Some TAs ---
for _ in range(5):
    course_id = random.choice(course_ids)
    user_id = random.choice(user_ids)
    
    # Check if user is already enrolled in this course
    cur.execute("""
        SELECT COUNT(*) FROM enrollments 
        WHERE user_id = %s AND course_id = %s AND type IN ('TeacherEnrollment', 'TaEnrollment')
    """, [user_id, course_id])
    
    if cur.fetchone()[0] == 0:
        section_ids = section_ids_by_course[course_id]
        section_id = random.choice(section_ids)
        created_at = fake.date_time_between(start_date="-1y", end_date="now")
        
        cur.execute("""
            INSERT INTO enrollments (
                user_id, course_id, type, workflow_state, created_at, updated_at,
                course_section_id, root_account_id, limit_privileges_to_course_section, role_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            user_id, course_id, "TaEnrollment", "active",
            created_at, created_at, section_id, ROOT_ACCOUNT_ID, False, role_map["TaEnrollment"]
        ])

# --- Commit and close ---
conn.commit()
cur.close()
conn.close()

print(f"✅ Generated:")
print(f"   - {NUM_USERS} users with login credentials")
print(f"   - {NUM_TERMS} enrollment terms")
print(f"   - {len(course_ids)} courses")
print(f"   - {sum(len(sections) for sections in section_ids_by_course.values())} course sections")
print(f"   - Multiple enrollments (students, teachers, TAs)")
print("\n📊 Query for People Page:")
print("""SELECT 
    u.id AS user_id,
    u.name,
    u.email,
    p.unique_id AS login_username,
    p.sis_user_id,
    cc.path AS contact_email,
    COUNT(DISTINCT e.course_id) AS courses_count
FROM users u
LEFT JOIN pseudonyms p ON p.user_id = u.id
LEFT JOIN communication_channels cc ON cc.user_id = u.id AND cc.path_type = 'email'
LEFT JOIN enrollments e ON e.user_id = u.id AND e.workflow_state = 'active'
GROUP BY u.id, u.name, u.email, p.unique_id, p.sis_user_id, cc.path
ORDER BY u.name;""")
print("\n📊 Query for Courses Page:")
print("""SELECT 
    c.id AS course_id,
    c.name AS course_name,
    c.course_code,
    et.name AS term,
    u.name AS teacher,
    COUNT(DISTINCT e.user_id) AS student_count
FROM courses c
LEFT JOIN enrollment_terms et ON et.id = c.enrollment_term_id
LEFT JOIN enrollments e ON e.course_id = c.id AND e.type = 'StudentEnrollment' AND e.workflow_state = 'active'
LEFT JOIN enrollments te ON te.course_id = c.id AND te.type = 'TeacherEnrollment'
LEFT JOIN users u ON u.id = te.user_id
GROUP BY c.id, c.name, c.course_code, et.name, u.name
ORDER BY c.name;""")