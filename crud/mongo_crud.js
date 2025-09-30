const { MongoClient } = require('mongodb');

// ---------------- CONFIGURAÇÃO ----------------
const url = 'mongodb://localhost:27017'; // Para MongoDB local
const client = new MongoClient(url);
const dbName = 'meuBanco';
const collectionName = 'usuarios';

// ---------------- FUNÇÃO DE CONEXÃO ----------------
async function conectar() {
    if (!client.isConnected()) await client.connect();
    const db = client.db(dbName);
    return db.collection(collectionName);
}

// ---------------- CREATE ----------------
async function criarUsuario(usuario) {
    const collection = await conectar();
    const resultado = await collection.insertOne(usuario);
    console.log('Usuário criado com ID:', resultado.insertedId);
}

// ---------------- READ ----------------
async function listarUsuarios() {
    const collection = await conectar();
    const usuarios = await collection.find({}).toArray();
    console.log('Todos os usuários:', usuarios);
}

async function buscarUsuarioPorNome(nome) {
    const collection = await conectar();
    const usuario = await collection.findOne({ nome });
    console.log('Usuário encontrado:', usuario);
}

// ---------------- UPDATE ----------------
async function atualizarUsuario(nome, novosDados) {
    const collection = await conectar();
    const resultado = await collection.updateOne(
        { nome },
        { $set: novosDados }
    );
    console.log(`Usuários modificados: ${resultado.modifiedCount}`);
}

// ---------------- DELETE ----------------
async function deletarUsuario(nome) {
    const collection = await conectar();
    const resultado = await collection.deleteOne({ nome });
    console.log(`Usuários deletados: ${resultado.deletedCount}`);
}

// ---------------- EXEMPLO DE USO ----------------
async function main() {
    // Criar usuários
    await criarUsuario({ nome: 'João', idade: 25, email: 'joao@email.com' });
    await criarUsuario({ nome: 'Maria', idade: 30, email: 'maria@email.com' });

    // Listar todos
    await listarUsuarios();

    // Buscar um usuário
    await buscarUsuarioPorNome('João');

    // Atualizar usuário
    await atualizarUsuario('João', { idade: 26 });

    // Deletar usuário
    await deletarUsuario('Maria');

    // Listar após alterações
    await listarUsuarios();

    // Fechar conexão
    await client.close();
}

main().catch(console.error);
