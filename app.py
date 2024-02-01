import pyodbc
from flask import Flask, request, redirect, url_for, render_template
from datetime import datetime

app = Flask(__name__)

server_name = r'localhost\APPWEB'
database_name = 'appWEB'
username = 'sa'
password = '12345'

def get_db_connection():
    conn = pyodbc.connect('Driver={SQL Server};'
                          f'Server={server_name};'
                          f'Database={database_name};'
                          f'UID={username};'
                          f'PWD={password};')
    return conn

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form['password']
        if password == 'gerencia':
            return redirect(url_for('ver_estimados_exportable'))
        elif password == 'operario':
            return redirect(url_for('insert_registro_produccion_diaria'))
        elif password == 'directorio':
            return redirect(url_for('ver_cumplimiento'))
        elif password == 'borrar':
            return redirect(url_for('gestionar_registro_produccion'))
        elif password == 'mejores':
            return redirect(url_for('ver_mejores_fincas'))
        else:
            return "Contraseña incorrecta", 401

    return render_template('login.html')

@app.route('/registro_produccion_diaria', methods=['GET', 'POST'])
def insert_registro_produccion_diaria():
    if request.method == 'POST':
        fecha_registro_str = request.form['fecha_registro']
        fecha_registro = datetime.strptime(fecha_registro_str, "%Y-%m-%d").date()

        cedula = request.form['cedula']
        finca = request.form['finca']
        tallos = request.form['tallos']

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            fecha_registro_db = fecha_registro.strftime("%Y-%m-%d")

            cursor.execute('''
                INSERT INTO RegistroProduccionDiaria (fecha_registro, cedula, finca, tallos)
                VALUES (?, ?, ?, ?)
            ''', (fecha_registro_db, cedula, finca, tallos))
            conn.commit()

            actualizar_resumen_mensual(conn, cursor, fecha_registro, finca, tallos)
            mes = fecha_registro.strftime("%Y-%m")
            actualizar_cumplimiento_produccion(mes, finca)

        except Exception as e:
            conn.rollback()
            print("Error al registrar la producción:", e)
            return "Error al registrar la producción", 500
        finally:
            cursor.close()
            conn.close()

        return redirect(url_for('insert_registro_produccion_diaria'))

    return render_template('registro_produccion_diaria.html')

def actualizar_resumen_mensual(conn, cursor, fecha, finca, tallos):
    try:
        mes = fecha.strftime("%Y-%m") 
        cursor.execute('''
            MERGE INTO ResumenProduccionMensual AS Dest
            USING (SELECT ? AS Mes, ? AS Finca, ? AS Tallos) AS Source
            ON Dest.mes = Source.Mes AND Dest.finca = Source.Finca
            WHEN MATCHED THEN 
                UPDATE SET 
                    Dest.tallos = Dest.tallos + Source.Tallos, 
                    Dest.tallos_exp = CAST((Dest.tallos + Source.Tallos) * 0.88 AS INT)
            WHEN NOT MATCHED THEN
                INSERT (mes, finca, tallos, tallos_exp)
                VALUES (Source.Mes, Source.Finca, Source.Tallos, CAST(Source.Tallos * 0.88 AS INT));
        ''', (mes, finca, int(tallos)))
        conn.commit()
    except Exception as e:
        print("Error al actualizar el resumen mensual:", e)
        conn.rollback()

def actualizar_cumplimiento_produccion(mes, finca):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            MERGE INTO CumplimientoProduccion AS CP
            USING (
                SELECT 
                    ? AS Mes, 
                    ? AS Finca, 
                    SUM(RPM.tallos_exp) AS TallosExp,
                    SUM(EE.estimados_tallos_exp) AS EstimadosTallosExp
                FROM 
                    ResumenProduccionMensual RPM
                INNER JOIN 
                    EstimadosExportable EE ON RPM.finca = EE.finca AND RPM.mes = EE.mes
                WHERE 
                    RPM.mes = ? AND RPM.finca = ?
                GROUP BY
                    RPM.mes, RPM.finca
            ) AS Source
            ON CP.mes = Source.Mes AND CP.finca = Source.Finca
            WHEN MATCHED THEN
                UPDATE SET 
                    CP.tallos = Source.TallosExp, 
                    CP.estimados_tallos_exp = Source.EstimadosTallosExp,
                    CP.cumplimiento = CAST(Source.TallosExp AS FLOAT) / NULLIF(Source.EstimadosTallosExp, 0)
            WHEN NOT MATCHED THEN
                INSERT (mes, finca, tallos, estimados_tallos_exp, cumplimiento)
                VALUES (Source.Mes, Source.Finca, Source.TallosExp, Source.EstimadosTallosExp, CAST(Source.TallosExp AS FLOAT) / NULLIF(Source.EstimadosTallosExp, 0));
        ''', (mes, finca, mes, finca))
        conn.commit()
    except Exception as e:
        print("Error al actualizar el cumplimiento de producción:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

@app.route('/gestionar_registro_produccion', methods=['GET', 'POST'])
def gestionar_registro_produccion():
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        id_registro = request.form['id']
        if not id_registro.isdigit():  # Verificar que el id es numérico
            return "ID inválido", 400

        try:
            cursor.execute('''
                SELECT 
                    CONVERT(char(7), fecha_registro, 120) as mes, 
                    finca, 
                    tallos 
                FROM RegistroProduccionDiaria 
                WHERE id = ?
            ''', (id_registro,))
            registro_borrado = cursor.fetchone()

            if registro_borrado:

                cursor.execute("DELETE FROM RegistroProduccionDiaria WHERE id = ?", (id_registro,))
                actualizar_resumen_y_cumplimiento(registro_borrado)

            conn.commit()
            mensaje = "Registro borrado correctamente."
        except Exception as e:
            conn.rollback()
            mensaje = f"Error al borrar el registro: {e}"
        finally:
            cursor.close()
            conn.close()
        return redirect(url_for('gestionar_registro_produccion', mensaje=mensaje))

    cursor.execute('SELECT id, fecha_registro, finca, tallos FROM RegistroProduccionDiaria')
    registros = cursor.fetchall()
    conn.close()
    return render_template('borrar.html', registros=registros)


@app.route('/ver_cumplimiento')
def ver_cumplimiento():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM CumplimientoProduccion')
    cumplimientos = cursor.fetchall()
    conn.close()

    return render_template('ver_cumplimiento.html', cumplimientos=cumplimientos)

@app.route('/estimados_exportable', methods=['GET', 'POST'])
def ver_estimados_exportable():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM EstimadosExportable')
    estimados_data = cursor.fetchall()
    conn.close()
    return render_template('estimados_exportable.html', estimados_data=estimados_data)


def actualizar_resumen_y_cumplimiento(registro_borrado):
    mes, finca, tallos_borrados = registro_borrado

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Actualizar ResumenProduccionMensual restando los tallos borrados
        cursor.execute('''
            UPDATE ResumenProduccionMensual
            SET tallos = tallos - ?, tallos_exp = tallos_exp - CAST(? * 0.88 AS INT)
            WHERE mes = ? AND finca = ?
        ''', (tallos_borrados, tallos_borrados, mes, finca))

        # Obtener los tallos_exp actualizados de ResumenProduccionMensual
        cursor.execute('''
            SELECT SUM(tallos_exp) FROM ResumenProduccionMensual WHERE mes = ? AND finca = ?
        ''', (mes, finca))
        tallos_exp_actual, = cursor.fetchone() or (0,)

        # Obtener los estimados_tallos_exp de EstimadosExportable
        cursor.execute('''
            SELECT SUM(estimados_tallos_exp) FROM EstimadosExportable WHERE mes = ? AND finca = ?
        ''', (mes, finca))
        estimados_tallos_exp, = cursor.fetchone() or (0,)

        # Calcular y actualizar el cumplimiento
        if estimados_tallos_exp > 0:
            cumplimiento = (tallos_exp_actual / estimados_tallos_exp)
        else:
            cumplimiento = 0  

        # Actualizar CumplimientoProduccion con los nuevos valores de tallos y cumplimiento
        cursor.execute('''
            UPDATE CumplimientoProduccion
            SET tallos = ?, cumplimiento = ?
            WHERE mes = ? AND finca = ?
        ''', (tallos_exp_actual, cumplimiento, mes, finca))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Error al actualizar resumen y cumplimiento:", e)
    finally:
        cursor.close()
        conn.close()

from flask import request

from flask import request

@app.route('/ver_mejores_fincas', methods=['GET', 'POST'])
def ver_mejores_fincas():
    conn = get_db_connection()
    cursor = conn.cursor()

    fechas = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12']
    fincas = ['TESSA', 'TESSA3', 'POSITANO', 'EC1', 'EC2', 'DALI', 'ARCOFLOR']

    promedio_cumplimiento = None
    nombre_finca = None

    if request.method == 'POST':
        selected_fecha_inicio = request.form['fecha_inicio']
        selected_fecha_fin = request.form['fecha_fin']
        selected_finca = request.form['finca']

        query = '''SELECT finca, AVG(cumplimiento) AS promedio_cumplimiento
                   FROM CumplimientoProduccion 
                   WHERE mes BETWEEN ? AND ? AND finca = ?
                   GROUP BY finca;
                '''

        cursor.execute(query, (selected_fecha_inicio, selected_fecha_fin, selected_finca))
        result = cursor.fetchone()

        if result:
            nombre_finca = result[0]
            promedio_cumplimiento = result[1]

    conn.close()

    return render_template('mejores.html', nombre_finca=nombre_finca, promedio_cumplimiento=promedio_cumplimiento, fechas=fechas, fincas=fincas)


@app.route('/')
def index():
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4040, debug=True)