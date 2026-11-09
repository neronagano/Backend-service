const form = document.querySelector('#task-form');
const titleInput = document.querySelector('#title');
const submitButton = document.querySelector('#submit');
const message = document.querySelector('#message');
const tasksList = document.querySelector('#tasks');

function showMessage(text, error = false) {
  message.textContent = text;
  message.className = error ? 'error' : '';
}

async function loadTasks() {
  const response = await fetch('/api/tasks');
  if (!response.ok) throw new Error('Не удалось загрузить список задач');
  const tasks = await response.json();
  tasksList.replaceChildren();
  for (const task of tasks) {
    const item = document.createElement('li');
    item.textContent = task.title;
    tasksList.append(item);
  }
  document.querySelector('#count').textContent = tasks.length;
  document.querySelector('#empty').hidden = tasks.length !== 0;
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const title = titleInput.value.trim();
  if (!title) {
    showMessage('Нужно указать название задачи', true);
    titleInput.focus();
    return;
  }
  submitButton.disabled = true;
  showMessage('Задача отправляется');
  try {
    const response = await fetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title }),
    });
    if (!response.ok) {
      throw new Error(response.status === 422
        ? 'Название должно содержать от 1 до 200 символов'
        : 'Не удалось добавить задачу');
    }
    titleInput.value = '';
    try {
      await loadTasks();
      showMessage('Задача добавлена');
    } catch {
      showMessage('Задача добавлена, но список не обновился. Нажмите «Обновить».', true);
    }
    titleInput.focus();
  } catch (error) {
    showMessage(error.message, true);
  } finally {
    submitButton.disabled = false;
  }
});

document.querySelector('#refresh').addEventListener('click', async () => {
  try {
    await loadTasks();
    showMessage('Список обновлён');
  } catch (error) {
    showMessage(error.message, true);
  }
});

async function initialize() {
  try {
    const response = await fetch('/apihealth');
    if (!response.ok || (await response.json()).status !== 'ok') throw new Error();
    document.querySelector('#health').textContent = 'Сервер доступен';
  } catch {
    document.querySelector('#health').textContent = 'Нет соединения с сервером';
  }
  try {
    await loadTasks();
  } catch (error) {
    showMessage(error.message, true);
  }
}

initialize();
