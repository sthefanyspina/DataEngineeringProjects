--Total de Vendas por Cliente
WITH vendas_cliente AS (
    SELECT f.id_cliente, SUM(f.valor_total) AS total_vendas
    FROM fact_vendas f
    GROUP BY f.id_cliente
)
SELECT c.nome, v.total_vendas
FROM vendas_cliente v
JOIN dim_cliente c ON v.id_cliente = c.id_cliente
ORDER BY v.total_vendas DESC
LIMIT 10;

--Total de Vendas por Produto
WITH vendas_cliente AS (
    SELECT f.id_cliente, SUM(f.valor_total) AS total_vendas
    FROM fact_vendas f
    GROUP BY f.id_cliente
)
SELECT c.nome, v.total_vendas
FROM vendas_cliente v
JOIN dim_cliente c ON v.id_cliente = c.id_cliente
ORDER BY v.total_vendas DESC
LIMIT 10;

--Receita Mensal
SELECT 
    t.ano, t.mes,
    SUM(f.valor_total) AS receita_mensal
FROM fact_vendas f
JOIN dim_tempo t ON f.id_tempo = t.id_tempo
GROUP BY t.ano, t.mes
ORDER BY t.ano, t.mes;

--Ticket Médio por Cliente
WITH ticket_cliente AS (
    SELECT id_cliente, AVG(valor_total) AS ticket_medio
    FROM fact_vendas
    GROUP BY id_cliente
)
SELECT c.nome, ROUND(t.ticket_medio, 2) AS ticket_medio
FROM ticket_cliente t
JOIN dim_cliente c ON t.id_cliente = c.id_cliente
ORDER BY t.ticket_medio DESC
LIMIT 10;

--Receita por Categoria de Produto
WITH receita_categoria AS (
    SELECT id_produto, SUM(valor_total) AS receita_total
    FROM fact_vendas
    GROUP BY id_produto
)
SELECT p.categoria, SUM(r.receita_total) AS receita_categoria
FROM receita_categoria r
JOIN dim_produto p ON r.id_produto = p.id_produto
GROUP BY p.categoria
ORDER BY receita_categoria DESC;

--Total de Vendas por Estado
WITH vendas_estado AS (
    SELECT id_cliente, SUM(valor_total) AS total_vendas
    FROM fact_vendas
    GROUP BY id_cliente
)
SELECT c.estado, SUM(v.total_vendas) AS receita_estado
FROM vendas_estado v
JOIN dim_cliente c ON v.id_cliente = c.id_cliente
GROUP BY c.estado
ORDER BY receita_estado DESC;

--Vendas Diárias
WITH receita_diaria AS (
    SELECT t.data, SUM(f.valor_total) AS receita_dia
    FROM fact_vendas f
    JOIN dim_tempo t ON f.id_tempo = t.id_tempo
    GROUP BY t.data
)
SELECT 
    data,
    receita_dia,
    ROUND(
        (receita_dia - LAG(receita_dia) OVER (ORDER BY data)) /
        NULLIF(LAG(receita_dia) OVER (ORDER BY data), 0) * 100, 2
    ) AS crescimento_pct
FROM receita_diaria
ORDER BY data;

--Produtos Mais Vendidos por Segmento
WITH vendas_segmento AS (
    SELECT id_cliente, id_produto, SUM(quantidade) AS qtd_vendida
    FROM fact_vendas
    GROUP BY id_cliente, id_produto
)
SELECT 
    c.segmento,
    p.nome AS produto,
    SUM(v.qtd_vendida) AS qtd_total
FROM vendas_segmento v
JOIN dim_cliente c ON v.id_cliente = c.id_cliente
JOIN dim_produto p ON v.id_produto = p.id_produto
GROUP BY c.segmento, p.nome
ORDER BY c.segmento, qtd_total DESC;

--Top 10 Clientes por Receita
SELECT 
    c.nome,
    SUM(f.valor_total) AS total_gasto
FROM fact_vendas f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
GROUP BY c.nome
ORDER BY total_gasto DESC
LIMIT 10;

--Médias Gerais
SELECT 
    ROUND(AVG(quantidade), 2) AS media_quantidade,
    ROUND(AVG(valor_total), 2) AS media_receita
FROM fact_vendas;

