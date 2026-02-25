/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conector;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author casaMesaRider
 */
public class Conector {

	private final String url = "mongodb://localhost:27017";
	private final String bd = "paisesDB";

	private MongoDatabase Mongodb;

	public Conector() {
	}

	/**
	 * Establece la conexión con el servidor de MongoDB local
	 *
	 * @return true si la conexión se realiza y el ping responde con éxito,
	 * false si ocurre cualquier MongoException (ej. servicio apagado).
	 */
	public boolean conectar() {
		boolean b = false;

		MongoClient mongo = MongoClients.create(url);

		if (mongo != null) {
			Mongodb = mongo.getDatabase(bd);
			b = true;
		} else {
			JOptionPane.showMessageDialog(null, "Error de conexión con MongoDB", "Error", JOptionPane.ERROR_MESSAGE);
		}

		return b;
	}

	/**
	 * Comprueba si un continente ya está registrado en la base de datos.
	 *
	 * @param nombrev El nombre del continente a buscar.
	 * @return true si el continente existe en la colección, false en caso
	 * contrario.
	 */
	public boolean existeContinente(String nombrev) {
		MongoCollection<Document> col = Mongodb.getCollection("continentes");
		return col.find(new Document("nombre", nombrev)).first() != null;
	}

	/**
	 * Inserta un nuevo documento en la colección de continentes.
	 *
	 * @param nombrev El nombre del nuevo continente a añadir.
	 * @return true si la inserción es exitosa, false si ocurre un error de
	 * escritura.
	 */
	public boolean anadirContinente(String nombrev) {
		boolean b = false;
		try {
			MongoCollection<Document> col = Mongodb.getCollection("continentes");
			col.insertOne(new Document("nombre", nombrev));
			b = true;
		} catch (MongoException e) {
			JOptionPane.showMessageDialog(null, "Error al escribir en Mongo: " + e.getMessage());
			b = false;
		}
		return b;
	}

	/**
	 * Recupera todos los continentes almacenados en la base de datos. Meto un
	 * String directamente el arrayList para simplificar ya que continente solo
	 * tiene nombre
	 *
	 * @return Un ArrayList de tipo String con los nombres de todos los
	 * continentes.
	 */
	public ArrayList<String> obtenerContinentes() {
		ArrayList<String> lista = new ArrayList<>();
		MongoCollection<Document> col = Mongodb.getCollection("continentes");
		for (Document doc : col.find()) {
			lista.add(doc.getString("nombre"));
		}
		return lista;
	}

	//OPERACIONES DE PAISES
	/**
	 * Comprueba si un país ya está registrado en la colección de países.
	 *
	 * @param nombrev El nombre del país a buscar.
	 * @return true si el país ya existe, false si no se encuentra.
	 */
	public boolean existePais(String nombrev) {
		MongoCollection<Document> col = Mongodb.getCollection("paises");
		return col.find(new Document("nombre", nombrev)).first() != null;
	}

	/**
	 * Inserta un nuevo país en la base de datos, enlazándolo con su continente
	 * correspondiente mediante el ObjectId de dicho continente (relación entre
	 * colecciones).
	 *
	 * @param nombrev El nombre del país a añadir.
	 * @param habitantesv La cantidad de habitantes del país (número entero).
	 * @param nombreContinentev El nombre del continente al que pertenece el
	 * país.
	 * @return true si el país se añade exitosamente, false si el continente no
	 * existe o hay un error.
	 */
	public boolean anadirPais(String nombrev, int habitantesv, String nombreContinentev) {
		boolean b = false;

		if (nombrev != null && !nombrev.isEmpty()) {
			MongoCollection<Document> paisesCol = Mongodb.getCollection("paises");
			MongoCollection<Document> continentesCol = Mongodb.getCollection("continentes");

			// 1. Buscamos el documento del continente por su nombre
			Document continenteDoc = continentesCol.find(new Document("nombre", nombreContinentev)).first();

			if (continenteDoc != null) {
				// 2. Extraemos su ObjectId
				ObjectId continenteId = continenteDoc.getObjectId("_id");

				try {
					// 3. Creamos el país referenciando el ObjectId del continente
					Document pais = new Document("nombre", nombrev)
							.append("habitantes", habitantesv)
							.append("continenteId", continenteId);

					paisesCol.insertOne(pais);
					b = true;
				} catch (MongoWriteException e) {
					if (e.getCode() == 11000) {
						System.out.println("Error: duplicidad en la clave");
					} else {
						System.out.println("Error de escritura en Mongo: " + e.getMessage());
					}
				}
			} else {
				System.out.println("El continente no existe en la base de datos.");
			}
		} else {
			System.out.println("Nombre del país sin valor");
		}

		return b;
	}

	/**
	 * Elimina un país de la base de datos buscando por su nombre.
	 *
	 * @param nombrev El nombre exacto del país que se desea eliminar.
	 * @return true si el país se encontró y eliminó con éxito, false en caso de
	 * no encontrarlo o fallar.
	 */
	public boolean eliminarPais(String nombrev) {
		boolean b = false;
		try {
			MongoCollection<Document> col = Mongodb.getCollection("paises");
			Document doc = new Document("nombre", nombrev);
			if (doc != null) {
				col.findOneAndDelete(doc);
				b = true;
			}
		} catch (MongoException e) {
			JOptionPane.showMessageDialog(null, "Error al borrar en Mongo: " + e.getMessage());
			b = false;
		}
		return b;

	}

	/**
	 * Recupera todos los paises almacenados en la base de datos.
	 *
	 * @return Un ArrayList de tipo Document
	 */
	public ArrayList<Document> obtenerPaises() {
		ArrayList<Document> lista = new ArrayList<>();
		MongoCollection<Document> col = Mongodb.getCollection("paises");
		for (Document doc : col.find()) {
			lista.add(doc);
		}
		return lista;
	}

	/**
	 * Recupera todos los países que pertenecen a un continente específico,
	 * buscando la relación mediante el ObjectId del continente, y los devuelve
	 * ordenados alfabéticamente por nombre.
	 *
	 * @param nombreContinentev El nombre del continente del cual queremos
	 * listar los países.
	 * @return Un ArrayList de Documentos BSON con la información de los países
	 * encontrados.
	 */
	public ArrayList<Document> obtenerPaisesPorContinente(String nombreContinentev) {
		ArrayList<Document> lista = new ArrayList<>();

		MongoCollection<Document> continentesCol = Mongodb.getCollection("continentes");
		// Buscamos el continente para obtener su _id
		Document continenteDoc = continentesCol.find(new Document("nombre", nombreContinentev)).first();

		if (continenteDoc != null) {
			ObjectId continenteId = continenteDoc.getObjectId("_id");

			MongoCollection<Document> paisesCol = Mongodb.getCollection("paises");
			// Buscamos los países que tengan ese ObjectId 
			FindIterable<Document> docs = paisesCol.find(new Document("continenteId", continenteId));

			for (Document doc : docs) {
				lista.add(doc);
			}
			// Ordenamos la lista usando un Comparator
			lista.sort((Document d1, Document d2) -> {
				String nombre1 = d1.getString("nombre");
				String nombre2 = d2.getString("nombre");
				return nombre1.compareToIgnoreCase(nombre2);
			});
		}

		return lista;

	}

}
