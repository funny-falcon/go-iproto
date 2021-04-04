package sbox

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/marshal"
)

/**
 * @brief Структура, описывающая вызов процедуры
 */
type RPCReq struct {
	/**
	 * @brief Имя вызываемой процедуры
	 */
	Name string

	/**
	 * @brief Параметры процедуры
	 *
	 * Все параметры передаются как строки
	 */
	Args []string
}

/**
 * @brief Вспомогательный метод для передачи строки в формате,
 *        который принимает octopus при разборе вызова процедуры
 *
 * К сожалению, чтобы определить эту функцию как метод для
 * marshal.Writer, придётся вносить изменения в основной код
 * драйвера, а этого хотелось бы избежать. Поэтому определяем
 * как локальную функцию
 */
func stringvar(w *marshal.Writer, s string) {
	w.Intvar(len(s))
	w.String(s)
}

/**
 * @brief Метод для маршаллинга вызова процедуры
 */
func (s RPCReq) IWrite(w *marshal.Writer) {
	//
	// Поскольку флаги всё равно игнорируются, передаём 0
	//
	w.IntUint32(0)

	//
	// Передаём имя процедуры
	//
	stringvar(w, s.Name)

	//
	// Передаём число аргументов процедуры
	//
	w.IntUint32(len(s.Args))

	//
	// Передаём аргументы процедуры
	//
	for _, v := range s.Args {
		stringvar(w, v)
	}
}

/**
 * @brief Код запроса для запуска процедуры
 */
func (s RPCReq) IMsg() iproto.RequestType {
	return 22
}
