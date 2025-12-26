import os
import hashlib
import functools
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, Response
from db_manager import SchoolDB
import rpc_handlers 
from xmlrpc.server import SimpleXMLRPCDispatcher
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'pi_secure_key')
logger = logging.getLogger(__name__)

# --- RPC REGISTRATION ---
rpc_dispatcher = SimpleXMLRPCDispatcher(allow_none=True)
rpc_dispatcher.register_function(rpc_handlers.rpc_login, 'login')
rpc_dispatcher.register_function(rpc_handlers.rpc_get_student_tps, 'get_student_tps')
rpc_dispatcher.register_function(rpc_handlers.rpc_get_submissions, 'get_submissions')
rpc_dispatcher.register_function(rpc_handlers.rpc_grade_submission, 'grade_submission')
rpc_dispatcher.register_function(rpc_handlers.rpc_get_teacher_data, 'get_teacher_data')

rpc_dispatcher.register_function(rpc_handlers.rpc_submit_rapport, 'submit_rapport')
rpc_dispatcher.register_function(rpc_handlers.rpc_get_session_students, 'get_session_students')
rpc_dispatcher.register_function(rpc_handlers.rpc_save_attendance, 'save_attendance')

@app.route('/RPC2', methods=['POST'])
def rpc_handler():
    return Response(rpc_dispatcher._marshaled_dispatch(request.data), mimetype='text/xml')

# --- WEB UI SECURITY ---
def login_required(role=None):
    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            if 'user_id' not in session: return redirect(url_for('login'))
            if role and session.get('role') != role: return redirect(url_for('login'))
            return f(*args, **kwargs)
        return wrapped
    return decorator

# --- AUTH ROUTES ---
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        with SchoolDB() as db:
            hpw = hashlib.sha256(request.form['password'].encode()).hexdigest()
            user = db.login(request.form['email'], hpw)
            if user:
                session.update({'user_id': user['id'], 'name': user['name'], 'role': user['role']})
                if user['role'] == 'Direction': return redirect(url_for('admin_dashboard'))
                # Dashboards for other roles can be added here
            flash("Invalid Credentials", "danger")
    return render_template('login.html')

# --- ADMIN USER MANAGEMENT (AJAX OPTIMIZED) ---
@app.route('/admin')
@login_required('Direction')
def admin_dashboard():
    with SchoolDB() as db:
        return render_template('admin.html', 
                               users=db.get_all_users_extended(), 
                               grouped_groups=db.get_groups_by_filiere(), 
                               modules=db.get_all_modules(), 
                               all_tps=db.get_all_tps_global())

@app.route('/admin/get_user/<int:user_id>')
@login_required('Direction')
def get_user(user_id):
    with SchoolDB() as db:
        return jsonify(db.get_user_details(user_id))

@app.route('/admin/create_user', methods=['POST'])
@login_required('Direction')
def create_user():
    role = request.form.get('role')
    extra = {
        'cne': request.form.get('cne'),
        'groupe_id': request.form.get('groupe_id'),
        'matricule': request.form.get('matricule')
    }
    with SchoolDB() as db:
        success = db.create_user_account(
            request.form['nom'], request.form['prenom'], 
            request.form['email'], request.form['password'], role, extra
        )
    return jsonify({'status': 'success' if success else 'error'})

@app.route('/admin/update_user', methods=['POST'])
@login_required('Direction')
def update_user():
    user_id = request.form.get('user_id')
    data = {
        'nom': request.form.get('nom'),
        'prenom': request.form.get('prenom'),
        'email': request.form.get('email'),
        'role': request.form.get('role'),
        'password': request.form.get('password'),
        'cne': request.form.get('cne'),
        'groupe_id': request.form.get('groupe_id'),
        'matricule': request.form.get('matricule')
    }
    with SchoolDB() as db:
        success = db.update_user(user_id, data)
    return jsonify({'status': 'success' if success else 'error'})

@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
@login_required('Direction')
def delete_user(user_id):
    with SchoolDB() as db:
        success = db.delete_user(user_id)
    return jsonify({'status': 'success' if success else 'error'})

# --- ASSIGNMENT MANAGEMENT ---
@app.route('/admin/assign_module', methods=['POST'])
@login_required('Direction')
def assign_module():
    with SchoolDB() as db:
        success = db.assign_formateur_to_module(
            request.form['formateur_id'], 
            request.form['groupe_id'], 
            request.form['module_id']
        )
    return jsonify({'status': 'success' if success else 'error'})

@app.route('/admin/get_assignments/<int:fid>')
@login_required('Direction')
def get_assignments(fid):
    with SchoolDB() as db:
        return jsonify(db.get_teacher_assignments_detailed(fid))

@app.route('/admin/delete_assignment/<int:aid>', methods=['POST'])
@login_required('Direction')
def delete_assignment(aid):
    with SchoolDB() as db:
        success = db.delete_assignment(aid)
    return jsonify({'status': 'success' if success else 'error'})

# --- ANALYTICS ---
@app.route('/analytics')
@login_required()
def analytics_dashboard():
    role = session.get('role')
    with SchoolDB() as db:
        teachers = []
        if role == 'Direction':
            with db.conn.cursor() as cursor:
                cursor.execute("SELECT UserID, Nom, Prenom FROM Utilisateur WHERE Role='Formateur'")
                teachers = [{"id": r.UserID, "name": f"{r.Nom} {r.Prenom}"} for r in cursor.fetchall()]
    return render_template('analytics.html', role=role, teachers=teachers)

@app.route('/api/analytics_data', methods=['POST'])
@login_required()
def get_analytics_data():
    target = session['user_id'] if session['role'] == 'Formateur' else request.json.get('formateur_id')
    if target == 'all': target = None
    
    with SchoolDB() as db:
        # Separate calls to prevent cursor state conflict
        stats = db.get_presence_stats(target)
        absences = db.get_absent_report(target)
        kpis = db.get_global_kpis(target)
        
        # Avoid division by zero for Avg Rate
        avg_rate = 0
        if stats:
            avg_rate = round(sum(s['rate'] for s in stats) / len(stats), 1)
        kpis['avg_rate'] = avg_rate
        
        return jsonify({'stats': stats, 'kpis': kpis, 'absences': absences})

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

if __name__ == '__main__':
    # Use threaded=True to help the Pi 1 B+ handle multiple background fetch requests
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=True)