const mqtt = require('mqtt');
const amqp = require('amqplib');
const ping = require('ping');

const mqttServer = 'mqtt://localhost:1883'; // Endereço do servidor MQTT
const mqttTopic = 'test_buffer'; // Tópico MQTT a ser assinado
const mqttCorruptTopic = 'corrupt_measurements'; // Tópico MQTT para mensagens corrompidas

const amqpServerUrl = 'amqp://W4nuCL2HK09PrG8H:7NXYX2gGYHGxCIBKoN3UtsLfRh@trends.injetoras.tcsapp.com.br:5672'; // URL do servidor AMQP
const amqpQueue = 'measurements'; // Nome da fila AMQP para mensagens válidas
const nomeUsuario = 'W4nuCL2HK09PrG8H'; // Nome de usuário AMQP
const senha = '7NXYX2gGYHGxCIBKoN3UtsLfRh'; // Senha AMQP

const bufferMaxSize = 1000000; // Tamanho máximo do buffer
const buffer = [];

const mqttClient = mqtt.connect(mqttServer);

let isAMQPConnected = false;

// Variável para controlar o envio das mensagens do buffer
let isBufferBeingProcessed = false;

// Função para configurar a conexão AMQP
async function setupAMQPConnection(serverUrl) {
  try {
    const amqpConnection = await amqp.connect(serverUrl);
    let channel = await amqpConnection.createChannel(); // Alterado para let

    // Declare a queue

    console.log('Conectado ao servidor AMQP');

    // Defina a variável isAMQPConnected como verdadeira após a conexão bem-sucedida
    isAMQPConnected = true;

    // Configurar um ouvinte de erro para lidar com desconexões inesperadas
    amqpConnection.on('error', (err) => {
      console.error('Erro na conexão AMQP:', err);
      // Defina isAMQPConnected como falso quando ocorrer um erro de conexão
      isAMQPConnected = false;
      // Limpar a referência ao canal
      channel = null;
      // Chame processBuffer() após a reconexão do AMQP
      processBuffer();
    });

    return { amqpConnection, channel };
  } catch (error) {
    console.error('Erro ao configurar a conexão AMQP:', error);
    // Defina isAMQPConnected como falso quando ocorrer um erro de conexão
    isAMQPConnected = false;
    return null;
  }
}

// Inicie a conexão AMQP
let amqpChannelInfo = null;

async function initAMQPConnection() {
  while (!amqpChannelInfo) {
    amqpChannelInfo = await setupAMQPConnection(amqpServerUrl);
    if (!amqpChannelInfo) {
      console.log('Tentando reconectar ao servidor AMQP em 5 segundos...');
      await new Promise((resolve) => setTimeout(resolve, 5000)); // Aguarde 5 segundos antes de tentar novamente
    }
  }
}

initAMQPConnection();

mqttClient.on('connect', () => {
  console.log('Conectado ao servidor MQTT');
  mqttClient.subscribe(mqttTopic);

  // Após a conexão MQTT ser estabelecida, envie mensagens pendentes do buffer
  processBuffer();
});

mqttClient.on('message', (topic, message) => {
  try {
    const parsedMessage = JSON.parse(message);

    // Verificar se há uma conexão com a internet.
    checkInternet()
      .then(hasInternet => {
        if (hasInternet) {
          if (isValidMeasurement(parsedMessage)) {
            // Enviar para o servidor AMQP de mensagens válidas
            sendToAMQP(amqpServerUrl, parsedMessage);
          } else {
            // Enviar para o servidor MQTT de mensagens corrompidas
            sendToMQTT(parsedMessage, mqttCorruptTopic);
          }
        } else {
          // Armazenar no buffer e manter o tamanho máximo
          buffer.push(parsedMessage);
          if (buffer.length > bufferMaxSize) {
            const removedMessage = buffer.shift(); // Remover a mensagem mais antiga se o buffer estiver cheio
            console.log('Mensagem removida do buffer (cheio):', removedMessage);
          }
          console.log('Mensagem armazenada no buffer:', parsedMessage);
        }
      })
      .catch(error => {
        console.error('Erro ao verificar a conectividade:', error);
      });
  } catch (error) {
    console.error('Erro ao analisar mensagem JSON:', error);
  }
});

async function sendToAMQP(serverUrl, message) {
  try {
    if (!amqpChannelInfo) {
      console.error('A conexão AMQP não está disponível. Ignorando mensagem:', message);
      return;
    }

    let { amqpConnection, channel } = amqpChannelInfo; // Alterado para let

    // Verifique o estado da conexão AMQP antes de enviar a mensagem
    if (!isAMQPConnected) {
      console.log('A conexão AMQP foi perdida. Tentando reconectar...');
      amqpChannelInfo = await setupAMQPConnection(serverUrl);
      if (!amqpChannelInfo) {
        console.error('Falha ao reconectar à conexão AMQP.');
        return;
      }

      // Atualize a referência ao canal após a reconexão
      ({ channel } = amqpChannelInfo);
    }

    // Publicar a mensagem na fila
    channel.sendToQueue(amqpQueue, Buffer.from(JSON.stringify(message)));
    console.log('Mensagem enviada para a fila AMQP:', message);
  } catch (error) {
    console.error('Erro ao enviar mensagem para a fila AMQP:', error);
    // Adicione lógica de tratamento de erro, se desejado
  }
}

function sendToMQTT(message, customTopic = mqttTopic) {
  try {
    const mqttClient = mqtt.connect(mqttServer);
    mqttClient.on('connect', () => {
      mqttClient.publish(customTopic, JSON.stringify(message));
      console.log(`Mensagem enviada para o tópico MQTT (${customTopic}):`, message);
      mqttClient.end();
    });
  } catch (error) {
    console.error(`Erro ao enviar mensagem para o tópico MQTT (${customTopic}):`, error);
  }
}

function isValidMeasurement(message) {
  // Verificar se a mensagem tem os campos esperados (id_variavel, valor, data_hora)
  return (
    message &&
    typeof message.id_variavel === 'number' &&
    typeof message.valor === 'number' &&
    typeof message.data_hora === 'number'
  );
}

function checkInternet() {
  return new Promise((resolve, reject) => {
    // Ping um servidor de sua escolha para verificar a conectividade com a internet
    const targetHost = 'www.google.com';
    ping.sys.probe(targetHost, (isAlive) => {
      resolve(isAlive);
    });
  });
}

// Função para processar o buffer e enviar mensagens pendentes
async function processBuffer() {
  if (!isAMQPConnected || isBufferBeingProcessed || buffer.length === 0) {
    return;
  }

  isBufferBeingProcessed = true;

  while (buffer.length > 0) {
    const message = buffer.shift();
    if (isValidMeasurement(message)) {
      sendToAMQP(amqpServerUrl, message);
    } else {
      sendToMQTT(message, mqttCorruptTopic);
    }
  }

  isBufferBeingProcessed = false;
}

// Este é apenas um exemplo de como você pode verificar periodicamente a conectividade com a internet
// Você pode implementar uma lógica mais robusta para verificar a conectividade
setInterval(() => {
  checkInternet()
    .then(hasInternet => {
      if (hasInternet && buffer.length > 0) {
        // Enviar mensagens pendentes quando a internet estiver disponível
        processBuffer();
      }
    })
    .catch(error => {
      console.error('Erro ao verificar a conectividade:', error);
    });
}, 5000); // Verificar a cada 5 segundos
