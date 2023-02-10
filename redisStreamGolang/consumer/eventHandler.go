package main

import (
	"fmt"
	prod "redisstreamgolang/producer"
)

func handleEvent(event prod.SomeEvent) error {

	fmt.Printf("Evento recebido!\nDados: id = %d descrição = %s horario %s",
		event.Id, event.Descricao, event.Horario.String())

	return nil
}
