# Desafio de Engenheiro de Dados

# Objetivo

O objetivo deste desafio é capturar, estruturar, armazenar e transformar dados de uma API instantânea. A API fornece informações de GPS dos veículos do BRT, geradas em tempo real no momento da consulta.

# API de Dados

Endpoint: [GET] https://dados.mobilidade.rio/gps/brt

A API retorna o último sinal transmitido por cada veículo do BRT.

# Instruções do Desafio

1. Extração e Carga com Prefect

    Construir um flow no Prefect que capture os dados a cada minuto.
  
    Salvar os dados extraídos em arquivos no formato CSV.
  
    Carregar os dados de forma incremental para uma tabela em um banco de dados PostgreSQL rodando localmente.

2. Transformações com DBT

    Criar uma tabela derivada utilizando modelos DBT.
    
    A tabela derivada deve conter:
    
    * ID do ônibus
    
    * Posição (latitude e longitude)
    
    * Velocidade
    
    * Tecnologias Utilizadas
    
    * Python
    
    * DBT (Data Build Tool)
    
    * Prefect (Versão 0.15.9)
    
    * Docker
    
    * PostgreSQL