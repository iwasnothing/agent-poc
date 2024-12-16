let socket;
let token;
let message_history = [];
let autolist = [];
let selectedIndex = -1; // Track the currently highlighted index

document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('loginPopup').style.display = 'block';
});

async function login() {
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    const response = await fetch('/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
    });

    if (response.ok) {
        const data = await response.json();
        token = data.access_token;
        document.getElementById('loginPopup').style.display = 'none';
        document.getElementById('chatArea').style.display = 'block';
        connectWebSocket(username,password);
    } else {
        alert('Login failed. Please try again.');
    }
}
function bot_message_box(content) {
    return `<div style="
    border-radius: 10px;
    background-color: #ffffff;
    padding: 20px;
    margin-right: auto;
    width: 800px;
    margin-top: 1px;
    margin-bottom: 1px;
    ">
        ${content}
    </div>
`
}

function user_message_box(content) {
    return `<div style="
    border-radius: 10px;
    background-color: #f0f0f0;
    padding: 20px;
    margin-left: auto;
    width: 800px;
    margin-top: 10px;
    margin-bottom: 10px;
    ">
        ${content}
    </div>
`
}
function render_message_history() {
    console.log(message_history)
    let combinedMessages = [];
    let currentMessage = message_history[0];
    for (let i = 1; i < message_history.length; i++) {
        if (message_history[i].role === currentMessage.role && message_history[i].type === currentMessage.type) {
            currentMessage.content += message_history[i].content;
        } else {
            combinedMessages.push(currentMessage);
            currentMessage = message_history[i];
        }
    }
    combinedMessages.push(currentMessage);
    message_history = combinedMessages;
    const messages = document.getElementById('messages');
    messages.innerHTML = '';
    for (const message of message_history) {
        const message_html_div = document.createElement('div');
        if (message.role === "user") {
            message_html_div.innerHTML = user_message_box(message.content);
        } else {
            message_html_div.innerHTML = bot_message_box(message.content);
        }
        messages.appendChild(message_html_div);
    }
}
function connectWebSocket(username,password) {
    socket = new WebSocket(`wss://${window.location.host}/ws?token=${token}`);

    socket.onopen = () => {
        console.log('WebSocket connection established');
        socket.send(`{"username": "${username}", "password": "${password}"}`)
    };
    
    socket.onmessage = (event) => {
        let message_type = "text"
        if (event.data.includes("iframe") || event.data.includes("img")) {
            message_type = "data"
        }
        const message_received = {
            role: "assistant",
            type: message_type,
            content: event.data
        };
        message_history.push(message_received);
        render_message_history();
    };

    socket.onclose = () => {
        console.log('WebSocket connection closed');
    };
}

function sendMessage() {
    const messageInput = document.getElementById('messageInput');
    const message = messageInput.value;
    let message_type = "text"
    const message_sent = {
        role: "user",
        type: message_type,
        content: message
    };
    message_history.push(message_sent);
    render_message_history();
    if (message && socket.readyState === WebSocket.OPEN) {
        socket.send(message);
        messageInput.value = '';
    }
}

document.getElementById('messageInput').addEventListener('input', handleKeyInput);

async function handleKeyInput() {
    const messageInput = document.getElementById('messageInput');
    const inputText = messageInput.value;
    const words = inputText.trim().split(/\s+/);
    const lastWord = words[words.length - 1];

    if (lastWord.length >= 2) {
        const response = await fetch('/search_mg', {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'X-API-Key': token
            },
            body: JSON.stringify({ mg_id: lastWord })
        });

        if (response.ok) {
            const results = await response.json();
            if (results.length > 0) {
                autolist = results;
                showPopup(results);
            }
        }
    }
}

function showPopup(results) {
    const popup = document.createElement('div');
    popup.id = 'auto-popup';
    popup.style.position = 'absolute';
    popup.style.top = `${document.getElementById('messageInput').getBoundingClientRect().top - 50}px`;
    popup.style.left = `${document.getElementById('messageInput').getBoundingClientRect().left}px`;
    popup.style.backgroundColor = '#fff';
    popup.style.border = '1px solid #ccc';
    popup.style.padding = '10px';
    popup.style.zIndex = '1000';
    popup.innerHTML = results.map(result => `<div>${result}</div>`).join('');
    
    document.body.appendChild(popup);

}


// New function to remove the popup window
function removePopup() {
    const popup = document.getElementById('auto-popup');
    if (popup) {
        document.body.removeChild(popup);
        autolist = [];
        selectedIndex = -1;
    }
}

function handleKeyNavigation(event) {
    const popup = document.getElementById('auto-popup');
    if (!popup) return;

    const items = popup.children;
    if (event.key === 'ArrowDown') {
        selectedIndex = (selectedIndex + 1) % items.length; // Move down
    } else if (event.key === 'ArrowUp') {
        selectedIndex = (selectedIndex - 1 + items.length) % items.length; // Move up
    } else if (event.key === 'Enter') {
        if (selectedIndex >= 0 && selectedIndex < items.length) {
            const selectedItem = items[selectedIndex].textContent;
            let user_query = document.getElementById('messageInput').value 
            console.log(user_query)
            user_query = user_query.trim().split(' ').slice(0, -1).join(' ') + ' ' + selectedItem;
            document.getElementById('messageInput').value = user_query;
            removePopup();
        }
    }
    else if (event.key === 'Escape') {
        removePopup();
    }

    // Highlight the selected item
    for (let i = 0; i < items.length; i++) {
        items[i].style.backgroundColor = (i === selectedIndex) ? '#e0e0e0' : '#fff'; // Change background color
    }
}

// Add event listener for key navigation
document.getElementById('messageInput').addEventListener('keydown', handleKeyNavigation);

// ... existing code ...
