import json
import os
import oracledb

DB_CONFIG = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "dsn": os.environ.get("DB_DSN")
}

PLSQL_BLOCK = """
DECLARE
    CURSOR c_participantes IS
        SELECT i.id, i.usuario_id, i.valor_pago, i.tipo
        FROM INSCRICOES i
        WHERE i.status = 'PRESENT'
        AND i.evento_id = 3;

    v_presencas NUMBER;
    v_cashback NUMBER;
    v_percentual NUMBER;

BEGIN
    FOR participante IN c_participantes LOOP

        SELECT COUNT(*)
        INTO v_presencas
        FROM INSCRICOES
        WHERE usuario_id = participante.usuario_id
        AND status = 'PRESENT';

        IF v_presencas > 3 THEN
            v_percentual := 0.25;
        ELSIF participante.tipo = 'VIP' THEN
            v_percentual := 0.20;
        ELSE
            v_percentual := 0.10;
        END IF;

        v_cashback := participante.valor_pago * v_percentual;

        UPDATE USUARIOS
        SET saldo = saldo + v_cashback
        WHERE id = participante.usuario_id;

    END LOOP;

    COMMIT;
END;
"""

def handler(request):
    try:
        body = json.loads(request.get("body") or "{}")
        acao = body.get("acao")

        conn = oracledb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if acao == "cashback":
            cursor.execute(PLSQL_BLOCK)
            conn.commit()
            return {
                "statusCode": 200,
                "body": json.dumps({"msg": "cashback aplicado"})
            }

        elif acao == "reset":
            cursor.execute("UPDATE USUARIOS SET saldo = 0")
            conn.commit()
            return {
                "statusCode": 200,
                "body": json.dumps({"msg": "saldos zerados"})
            }

        elif acao == "listar":
            cursor.execute("""
            SELECT 
                u.nome,
                u.email,
                u.saldo,
                NVL(MAX(CASE WHEN i.status = 'PRESENT' THEN i.tipo END), 'NORMAL'),
                NVL(SUM(CASE WHEN i.status = 'PRESENT' THEN i.valor_pago END), 0),
                COUNT(CASE WHEN i.status = 'PRESENT' THEN 1 END),
                CASE 
                    WHEN COUNT(CASE WHEN i.status = 'PRESENT' THEN 1 END) > 3 THEN 25
                    WHEN MAX(CASE WHEN i.status = 'PRESENT' THEN i.tipo END) = 'VIP' THEN 20
                    ELSE 10
                END
            FROM USUARIOS u
            LEFT JOIN INSCRICOES i ON u.id = i.usuario_id
            GROUP BY u.nome, u.email, u.saldo
            ORDER BY u.nome
            """)

            data = cursor.fetchall()

            return {
                "statusCode": 200,
                "body": json.dumps(data)
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"erro": str(e)})
        }

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass