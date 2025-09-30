import redis

# Conexão com o Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Função para criar usuário
def create_user(user_id, name, age, email):
    key = f"user:{user_id}"
    r.hset(key, mapping={"name": name, "age": age, "email": email})
    r.sadd("users", user_id)  # adiciona o ID ao set de usuários
    print(f"Usuário {name} criado com ID {user_id}.")

# Função para ler usuário
def get_user(user_id):
    key = f"user:{user_id}"
    user = r.hgetall(key)
    if not user:
        return None
    return {k.decode(): v.decode() for k, v in user.items()}

# Função para atualizar usuário
def update_user(user_id, field, value):
    key = f"user:{user_id}"
    if r.exists(key):
        r.hset(key, field, value)
        print(f"Usuário {user_id} atualizado: {field} = {value}")
    else:
        print("Usuário não encontrado.")

# Função para deletar usuário
def delete_user(user_id):
    key = f"user:{user_id}"
    r.delete(key)
    r.srem("users", user_id)
    print(f"Usuário {user_id} deletado.")

# Função para listar todos os usuários
def list_users():
    user_ids = r.smembers("users")
    all_users = []
    for uid in user_ids:
        uid = uid.decode()
        user = get_user(uid)
        if user:
            all_users.append({"id": uid, **user})
    return all_users

# ==========================
# Exemplo de uso
# ==========================

create_user(1, "Sthefany", 25, "sthefany@example.com")
create_user(2, "Carlos", 30, "carlos@example.com")

print("Todos os usuários:")
print(list_users())

print("Atualizando usuário 1...")
update_user(1, "age", 26)

print("Usuário 1:")
print(get_user(1))

print("Deletando usuário 2...")
delete_user(2)

print("Todos os usuários restantes:")
print(list_users())
