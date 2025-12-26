from db_manager import SchoolDB

# --- AUTHENTICATION ---
def rpc_login(email, password_hash):
    with SchoolDB() as db: 
        return db.login(email, password_hash)

# --- STUDENT PORTAL FUNCTIONS ---
def rpc_get_student_tps(student_id):
    """ Returns TPs specific to the student's group """
    with SchoolDB() as db:
        u = db.get_user_details(student_id)
        if u and u.get('groupe_id'):
            return db.get_tps_for_student(u['groupe_id'])
        return []

def rpc_submit_rapport(tp_id, student_id, file_data_base64, file_name, file_type):
    """ RPC version of TP submission (accepts base64 from Django) """
    import base64
    try:
        file_bytes = base64.b64decode(file_data_base64)
        with SchoolDB() as db:
            return db.submit_rapport_file(tp_id, student_id, file_bytes, file_name, file_type)
    except Exception:
        return False

# --- FORMATEUR PORTAL FUNCTIONS ---
def rpc_get_teacher_data(fid):
    """ Returns both assignments (for selectors) and history (for the table) """
    with SchoolDB() as db:
        assignments = db.get_teacher_modules(fid)
        history = db.get_formateur_history_mixed(fid)
        return {'assignments': assignments, 'history': history}

def rpc_get_submissions(tp_id):
    """ Get student reports for a specific TP to be graded """
    with SchoolDB() as db: 
        return db.get_submissions_for_tp(tp_id)

def rpc_grade_submission(sid, grade):
    """ Save a grade for a student submission """
    with SchoolDB() as db: 
        return db.save_grade(sid, grade)

def rpc_get_session_students(group_id, seance_id):
    """ Used for the presence marking interface """
    with SchoolDB() as db:
        return db.get_students_with_presence(group_id, seance_id)

def rpc_save_attendance(seance_id, presence_list):
    """ Saves bulk attendance data """
    with SchoolDB() as db:
        return db.save_bulk_presence(seance_id, presence_list)