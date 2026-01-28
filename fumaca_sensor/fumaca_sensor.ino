// import statements allow for different types of sensors or controllers to be used
#include <ESP8266WiFi.h>
#include <Arduino.h>

#define LENG 31   // 0x42 + 31 bytes equal to 32 bytes
unsigned char buf[LENG];

// these declarations are used to track the values to then plot into thingspeak. upon initalization they'll be set to 0 as a baseline.
int PM01Value = 0;
int PM2_5Value = 0;
int PM10Value = 0;

// allow for data to be sent to your computer via wifi
const char *ssid = "YOUR_WIFI_NAME";
const char *password = "YOUR_WIFI_PASSWORD";

String serverName = "https://api.thingspeak.com/update?api_key=YOUR_WRITE_KEY";
WiFiClient client;

unsigned long lastTime = 0;
unsigned long timerDelay = 60000;

// the setup loop's purpose is just to ensure connectivity and that your wifi is working
void setup() {
    Serial.begin(9600);

    WiFi.begin(ssid, password);
    Serial.println("Connecting");
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("");
    Serial.print("Connected to WiFi network with IP Address: ");
    Serial.println(WiFi.localIP());
}

void loop() {
    // starts to read when it gets data from buffer
    if (Serial.find(0x42)) {
        Serial.readBytes(buf, LENG);

        // reads the sensor value in this loop and uses previous variable to keep track of it
        if (buf[0] == 0x4d) {
            if (checkValue(buf, LENG)) {
                PM01Value = transmitPM01(buf);  // count PM1.0 value of the air detector module
                PM2_5Value = transmitPM2_5(buf);  // count PM2.5 value of the air detector module
                PM10Value = transmitPM10(buf);  // count PM10 value of the air detector module
            }
        }
    }

// prints out the values read from the sensor out to your computer
    static unsigned long OledTimer = millis();
    if (millis() - OledTimer >= 1000) {
        OledTimer = millis();

        Serial.print("PM1.0: ");
        Serial.print(PM01Value);
        Serial.println("  ug/m3");

        Serial.print("PM2.5: ");
        Serial.print(PM2_5Value);
        Serial.println("  ug/m3");

        Serial.print("PM10 : ");
        Serial.print(PM10Value);
        Serial.println("  ug/m3");
        Serial.println();

        // if able to send data over to thingspeak, it will do so here, and allow it to be displayed. You can look at ts api for clarification as needed
        if (client.connect(server, 80)) {
            String postStr = apiKey;
            postStr += "&field1=";
            postStr += String(PM01Value);
            postStr += "&field2=";
            postStr += String(PM2_5Value);
            postStr += "&field3=";
            postStr += String(PM10Value);
            postStr += "\r\n\r\n";

            client.print("POST /update HTTP/1.1\n");
            client.print("Host: api.thingspeak.com\n");
            client.print("Connection: close\n");
            client.print("X-THINGSPEAKAPIKEY: " + apiKey + "\n");
            client.print("Content-Type: application/x-www-form-urlencoded\n");
            client.print("Content-Length: ");
            client.print(postStr.length());
            client.print("\n\n");
            client.print(postStr);
        }
        client.stop();
    }
}

char checkValue(unsigned char *thebuf, char leng) {
    char receiveflag = 0;
    int receiveSum = 0;

    for (int i = 0; i < (leng - 2); i++) {
        receiveSum = receiveSum + thebuf[i];
    }
    receiveSum = receiveSum + 0x42;

    // check the serial data
    if (receiveSum == ((thebuf[leng - 2] << 8) + thebuf[leng - 1])) {
        receiveSum = 0;
        receiveflag = 1;
    }
    return receiveflag;
}

int transmitPM01(unsigned char *thebuf) {
    int PM01Val;
    PM01Val = ((thebuf[3] << 8) + thebuf[4]);  // count PM1.0 value of the air detector module
    return PM01Val;
}

// transmit PM Value to PC
int transmitPM2_5(unsigned char *thebuf) {
    int PM2_5Val;
    PM2_5Val = ((thebuf[5] << 8) + thebuf[6]);  // count PM2.5 value of the air detector module
    return PM2_5Val;
}

// transmit PM Value to PC
int transmitPM10(unsigned char *thebuf) {
    int PM10Val;
    PM10Val = ((thebuf[7] << 8) + thebuf[8]);  // count PM10 value of the air detector module
    return PM10Val;
}